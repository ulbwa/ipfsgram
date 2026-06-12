package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/carpack"
	"github.com/ulbwa/ipfsgram/internal/model"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

const (
	defaultCarMaxSize    = 15 << 20
	defaultWarnThreshold = 0.9
)

// newAddCmd returns the top-level `ipfsgram add` command.
func newAddCmd() *cobra.Command {
	var (
		cidArg string
		name   string
	)
	cmd := &cobra.Command{
		Use:   "add [path]",
		Short: "Опубликовать контент: локальный файл или DAG по CID из сети IPFS",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 1) == (cidArg != "") {
				return errors.New("укажите либо путь к файлу, либо --cid")
			}
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			tr, err := e.transport(ctx)
			if err != nil {
				return err
			}

			carMaxSize, err := e.Config.GetInt64(ctx, "car_max_size")
			if err != nil {
				if !errors.Is(err, repo.ErrNotFound) {
					return err
				}
				carMaxSize = defaultCarMaxSize
			}
			warnThreshold, err := e.Config.GetFloat64(ctx, "channel_warn_threshold")
			if err != nil {
				if !errors.Is(err, repo.ErrNotFound) {
					return err
				}
				warnThreshold = defaultWarnThreshold
			}

			// 1. Block source: local UnixFS import or network fetch.
			var (
				root    cid.Cid
				entries []blockEntry
				pinName = name
			)
			if cidArg != "" {
				root, err = cid.Decode(cidArg)
				if err != nil {
					return fmt.Errorf("некорректный CID %q: %w", cidArg, err)
				}
				fmt.Fprintf(stdout, "скачиваем DAG %s из сети IPFS...\n", root)
				entries, err = fetchFromNetwork(ctx, root)
				if err != nil {
					return err
				}
				if pinName == "" {
					pinName = root.String()
				}
			} else {
				root, entries, err = importLocalFile(ctx, args[0])
				if err != nil {
					return err
				}
				if pinName == "" {
					pinName = filepath.Base(args[0])
				}
			}

			var totalSize int64
			allCIDs := make([][]byte, len(entries))
			for i, ent := range entries {
				allCIDs[i] = ent.cid.Bytes()
				totalSize += int64(len(ent.data))
			}

			// Advisory-lock protocol vs `ipfsgram gc`: gc takes the lock in
			// EXCLUSIVE mode while it deletes unpinned cars; add takes it in
			// SHARED mode for its critical section — from the moment existing
			// blocks are read (dedup decision) until the pin row exists.
			// Otherwise gc could delete an unpinned car whose blocks add just
			// decided to skip, leaving the new pin referencing vanished
			// blocks. Shared locks don't block each other, so parallel adds
			// stay concurrent; only gc is mutually excluded. The lock is
			// transaction-scoped, so we hold a dedicated transaction open for
			// the duration (the repos keep using the pool for their own
			// statements — the lock only needs to be held, not shared).
			// It is released automatically when the tx ends at process exit.
			lockTx, err := e.db.BeginTxx(ctx, nil)
			if err != nil {
				return err
			}
			defer lockTx.Rollback() //nolint:errcheck // releases the advisory lock
			if _, err := lockTx.ExecContext(ctx,
				`SELECT pg_advisory_xact_lock_shared($1)`, gcAdvisoryLockID); err != nil {
				return fmt.Errorf("advisory lock: %w", err)
			}

			// 2. Dedup: which blocks already live in reachable published cars.
			existing, err := e.Blocks.Existing(ctx, allCIDs)
			if err != nil {
				return err
			}
			statuses, checks, err := e.checkExistingCars(ctx, tr, existing)
			if err != nil {
				return err
			}
			plan := planDedup(existing, statuses, checks)
			for _, carID := range plan.DeleteCars {
				log.Warn().Int64("car_id", carID).
					Msg("сообщение CAR'а физически удалено — записи удаляются, блоки будут переотправлены")
				if err := e.Cars.Delete(ctx, carID); err != nil && !errors.Is(err, repo.ErrNotFound) {
					return err
				}
			}
			for carID, st := range plan.SetStatus {
				log.Warn().Int64("car_id", carID).Str("status", string(st)).
					Msg("CAR недоступен — блоки будут переотправлены")
				if err := e.Cars.SetStatus(ctx, carID, st); err != nil && !errors.Is(err, repo.ErrNotFound) {
					return err
				}
			}

			var toUpload []blockEntry
			for _, ent := range entries {
				if !plan.Skip[string(ent.cid.Bytes())] {
					toUpload = append(toUpload, ent)
				}
			}

			// 3. Pack and upload.
			if len(toUpload) > 0 {
				if err := e.packAndUpload(ctx, tr, root, toUpload, existing, carMaxSize, warnThreshold); err != nil {
					return err
				}
			}

			// 4. Pin.
			if err := e.Pins.Create(ctx, root.Bytes(), pinName, totalSize, allCIDs); err != nil {
				return err
			}
			fmt.Fprintln(stdout, root.String())
			return nil
		},
	}
	cmd.Flags().StringVar(&cidArg, "cid", "", "скачать DAG по CID из сети IPFS вместо локального файла")
	cmd.Flags().StringVar(&name, "name", "", "имя пина (по умолчанию имя файла или CID)")
	return cmd
}

// checkExistingCars loads the cars referenced by the existing blocks and, for
// published ones, probes their Telegram messages.
func (e *env) checkExistingCars(
	ctx context.Context, tr tg.Transport, existing map[string]model.BlockRef,
) (map[int64]model.CarStatus, map[int64]carCheck, error) {
	carIDs := make(map[int64]bool)
	for _, ref := range existing {
		carIDs[ref.CarID] = true
	}
	statuses := make(map[int64]model.CarStatus, len(carIDs))
	checks := make(map[int64]carCheck, len(carIDs))
	if len(carIDs) == 0 {
		return statuses, checks, nil
	}

	bots, err := e.Bots.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	channels, err := e.Channels.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	chByID := make(map[int64]model.Channel, len(channels))
	for _, ch := range channels {
		chByID[ch.ID] = ch
	}

	lc := selector.NewLoadCounter()
	for carID := range carIDs {
		car, err := e.Cars.Get(ctx, carID)
		if errors.Is(err, repo.ErrNotFound) {
			continue // concurrently removed; its blocks cascaded away too
		}
		if err != nil {
			return nil, nil, err
		}
		statuses[carID] = car.Status
		if car.Status != model.CarPublished {
			continue
		}
		check, err := e.probeCarMessage(ctx, tr, car, chByID[car.ChannelID], bots, lc)
		if err != nil {
			return nil, nil, err
		}
		checks[carID] = check
	}
	return statuses, checks, nil
}

// probeCarMessage checks whether the car's Telegram message is alive, trying
// bots until one succeeds or none remain. Flood-waited bots are marked in the
// database and skipped; when every eligible bot is merely flood-waited, the
// probe waits for the earliest expiry and retries instead of misclassifying a
// healthy car as no_bot_access (which would force a needless re-upload). The
// returned error is non-nil only when the wait was interrupted (context
// cancelled).
func (e *env) probeCarMessage(
	ctx context.Context, tr tg.Transport,
	car model.Car, ch model.Channel,
	bots []model.Bot, lc *selector.LoadCounter,
) (carCheck, error) {
	if car.MessageID == nil {
		return checkNoAccess, nil
	}
	members, err := e.Channels.MembersOf(ctx, car.ChannelID)
	if err != nil {
		log.Warn().Err(err).Int64("car_id", car.ID).Msg("не удалось получить членство ботов")
		return checkNoAccess, nil
	}
	fileIDs, err := e.Cars.FileIDs(ctx, car.ID)
	if err != nil {
		log.Warn().Err(err).Int64("car_id", car.ID).Msg("не удалось получить file_id")
		fileIDs = nil
	}

	remaining := append([]model.Bot(nil), bots...)
	for {
		bot, _, err := selector.PickDownloadBot(time.Now(), remaining, members, fileIDs, lc)
		if err != nil {
			// All eligible bots may be temporarily flood-waited: wait the
			// earliest expiry out and retry rather than declaring no access.
			earliest := earliestRecovery(remaining, members, func(m model.BotChannel) bool {
				return m.Member && m.CanRead
			})
			if earliest == nil {
				log.Warn().Int64("car_id", car.ID).
					Msg("ни один бот не может проверить сообщение CAR'а — считаем no_bot_access")
				return checkNoAccess, nil
			}
			if werr := waitForFloodWait(ctx, *earliest); werr != nil {
				return checkNoAccess, werr
			}
			continue
		}

		cerr := tr.CheckMessage(ctx, bot.Token, ch.TgID, *car.MessageID)
		var fw *tg.FloodWaitError
		switch {
		case cerr == nil:
			lc.Record(bot.ID)
			return checkOK, nil
		case errors.Is(cerr, tg.ErrMessageDeleted):
			return checkDeleted, nil
		case errors.Is(cerr, tg.ErrTooLarge):
			return checkTooLarge, nil
		case errors.As(cerr, &fw):
			lc.RecordError(bot.ID)
			until := time.Now().Add(fw.RetryAfter)
			if err := e.Bots.SetUnavailableUntil(ctx, bot.ID, until); err != nil {
				log.Warn().Err(err).Msg("не удалось записать flood-wait")
			}
			// Keep the bot in the candidate list with its expiry recorded so
			// the selector skips it now but can pick it again after the wait.
			markUnavailable(remaining, bot.ID, until)
		case errors.Is(cerr, tg.ErrNoAccess):
			lc.RecordError(bot.ID)
			remaining = removeBot(remaining, bot.ID)
		default:
			lc.RecordError(bot.ID)
			log.Warn().Err(cerr).Int64("car_id", car.ID).Str("bot", bot.Username).
				Msg("ошибка проверки сообщения, пробуем другого бота")
			remaining = removeBot(remaining, bot.ID)
		}
	}
}

// markUnavailable records the flood-wait expiry on the in-memory candidate
// list so the selector skips the bot until it recovers.
func markUnavailable(bots []model.Bot, botID int64, until time.Time) {
	for i := range bots {
		if bots[i].ID == botID {
			bots[i].UnavailableUntil = &until
			return
		}
	}
}

// removeBot returns bots without the given ID.
func removeBot(bots []model.Bot, id int64) []model.Bot {
	out := bots[:0]
	for _, b := range bots {
		if b.ID != id {
			out = append(out, b)
		}
	}
	return out
}

// packAndUpload packs the blocks into rotating CAR files and publishes each
// of them. Partial failure leaves pending car rows for `ipfsgram doctor`.
func (e *env) packAndUpload(
	ctx context.Context, tr tg.Transport,
	root cid.Cid, entries []blockEntry,
	existing map[string]model.BlockRef,
	carMaxSize int64, warnThreshold float64,
) error {
	tmpDir, err := os.MkdirTemp("", "ipfsgram-add-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	packer, err := carpack.NewRotatingPacker(tmpDir, carMaxSize, root)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if err := packer.Add(ent.cid, ent.data); err != nil {
			return err
		}
	}
	packed, err := packer.Finish()
	if err != nil {
		return err
	}

	shortRoot := root.String()
	if len(shortRoot) > 16 {
		shortRoot = shortRoot[len(shortRoot)-16:]
	}

	lc := selector.NewLoadCounter()
	for i, pc := range packed {
		name := fmt.Sprintf("%s-%06d.car", shortRoot, i+1)
		if err := e.uploadCar(ctx, tr, pc, name, existing, warnThreshold, lc); err != nil {
			return err
		}
	}
	return nil
}

// uploadCar publishes one packed CAR: picks a channel and bot, creates the
// pending car row, uploads, marks published and records the block rows.
func (e *env) uploadCar(
	ctx context.Context, tr tg.Transport,
	pc carpack.PackedCar, name string,
	existing map[string]model.BlockRef,
	warnThreshold float64, lc *selector.LoadCounter,
) error {
	channels, err := e.Channels.List(ctx)
	if err != nil {
		return err
	}
	ch, warn, err := selector.PickChannel(channels, warnThreshold)
	if errors.Is(err, selector.ErrNoChannelSpace) {
		return errors.New("нет каналов со свободным местом: добавьте канал (ipfsgram channel add)")
	}
	if err != nil {
		return err
	}
	if warn {
		log.Warn().Str("channel", ch.Title).
			Int64("count", ch.MessageCount).Int64("limit", ch.MessageLimit).
			Msg("канал почти заполнен")
	}
	members, err := e.Channels.MembersOf(ctx, ch.ID)
	if err != nil {
		return err
	}

	carID, err := e.Cars.CreatePending(ctx, ch.ID, pc.Size, len(pc.Blocks))
	if err != nil {
		return err
	}

	for {
		bots, err := e.Bots.List(ctx)
		if err != nil {
			return err
		}
		bot, err := selector.PickUploadBot(time.Now(), bots, members, lc)
		if errors.Is(err, selector.ErrNoBotAvailable) {
			earliest := earliestRecovery(bots, members, func(m model.BotChannel) bool {
				return m.Member && m.CanPost
			})
			if earliest == nil {
				return fmt.Errorf("нет доступных ботов для канала «%s»", ch.Title)
			}
			if werr := waitForFloodWait(ctx, *earliest); werr != nil {
				return werr
			}
			continue
		}
		if err != nil {
			return err
		}

		f, err := os.Open(pc.Path)
		if err != nil {
			return err
		}
		res, uerr := tr.Upload(ctx, bot.Token, ch.TgID, name, pc.Size, f)
		f.Close()

		var fw *tg.FloodWaitError
		if errors.As(uerr, &fw) {
			lc.RecordError(bot.ID)
			until := time.Now().Add(fw.RetryAfter)
			log.Warn().Str("bot", bot.Username).Dur("retry_after", fw.RetryAfter).
				Msg("flood-wait при загрузке, пробуем другого бота")
			if err := e.Bots.SetUnavailableUntil(ctx, bot.ID, until); err != nil {
				log.Warn().Err(err).Msg("не удалось записать flood-wait")
			}
			continue
		}
		if uerr != nil {
			lc.RecordError(bot.ID)
			// Pending car row stays; `ipfsgram doctor` cleans it up.
			return fmt.Errorf("загрузка %s: %w", name, uerr)
		}
		lc.Record(bot.ID)

		if err := e.Cars.MarkPublished(ctx, carID, res.MessageID); err != nil {
			return err
		}
		if res.FileID != "" {
			if err := e.Cars.UpsertFileID(ctx, carID, bot.ID, res.FileID); err != nil {
				return err
			}
		}
		if err := e.Channels.IncrementMessageCount(ctx, ch.ID); err != nil {
			return err
		}

		var newRefs, repointRefs []model.BlockRef
		for _, b := range pc.Blocks {
			ref := model.BlockRef{
				CID: b.CID.Bytes(), CarID: carID, Offset: b.Offset, Length: b.Length,
			}
			if _, ok := existing[string(ref.CID)]; ok {
				repointRefs = append(repointRefs, ref)
			} else {
				newRefs = append(newRefs, ref)
			}
		}
		if err := e.Blocks.InsertBatch(ctx, newRefs); err != nil {
			return err
		}
		if err := e.Blocks.Repoint(ctx, repointRefs); err != nil {
			return err
		}
		return nil
	}
}

// earliestRecovery returns the earliest flood-wait expiry among active bots
// whose channel membership satisfies eligible, or nil when no such bot exists
// (i.e. waiting would never help).
func earliestRecovery(bots []model.Bot, members []model.BotChannel, eligible func(model.BotChannel) bool) *time.Time {
	ok := make(map[int64]bool, len(members))
	for _, m := range members {
		if eligible(m) {
			ok[m.BotID] = true
		}
	}
	var earliest *time.Time
	for _, b := range bots {
		if !b.Active || !ok[b.ID] || b.UnavailableUntil == nil {
			continue
		}
		if earliest == nil || b.UnavailableUntil.Before(*earliest) {
			t := *b.UnavailableUntil
			earliest = &t
		}
	}
	return earliest
}

// waitForFloodWait sleeps until the given time, printing progress. It returns
// the context error when cancelled mid-wait.
func waitForFloodWait(ctx context.Context, until time.Time) error {
	for {
		remaining := time.Until(until)
		if remaining <= 0 {
			return nil
		}
		fmt.Fprintf(stdout, "все боты во flood-wait, ждём %d сек\n", int(remaining.Seconds())+1)
		timer := time.NewTimer(min(remaining, 10*time.Second))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
