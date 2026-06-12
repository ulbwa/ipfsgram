package publish

import "github.com/ulbwa/ipfsgram/internal/domain"

// carCheck is the result of probing a published car's Telegram message.
type carCheck int

const (
	checkOK       carCheck = iota // message alive, file reachable
	checkDeleted                  // message physically deleted — unrecoverable
	checkNoAccess                 // no usable bot / bot lost access — recoverable
	checkTooLarge                 // file exceeds current transport limit — recoverable
)

// dedupPlan is the pure outcome of the dedup decision for Publish.
type dedupPlan struct {
	// Skip holds CIDs (string(cid.Bytes())) already available in live published
	// cars: they are NOT re-uploaded.
	Skip map[string]bool
	// Reupload holds CIDs that exist in the database but must be re-uploaded
	// (their rows are Repointed to the new car after upload).
	Reupload map[string]bool
	// DeleteCars lists cars whose message is physically deleted: their rows are
	// removed from the database immediately.
	DeleteCars []int64
	// SetStatus maps cars to the recoverable-unavailability status they should
	// be marked with.
	SetStatus map[int64]domain.CarStatus
}

// planDedup decides, for every block already present in the database, whether it
// can be skipped (its car is published and its message verified alive) or must
// be re-uploaded. statuses holds the current status of every referenced car;
// checks holds the message-probe result for cars that were published. Pure
// function: no I/O, unit-testable.
func planDedup(
	existing map[string]domain.Block,
	statuses map[int64]domain.CarStatus,
	checks map[int64]carCheck,
) dedupPlan {
	plan := dedupPlan{
		Skip:      make(map[string]bool),
		Reupload:  make(map[string]bool),
		SetStatus: make(map[int64]domain.CarStatus),
	}

	deleted := make(map[int64]bool)
	for carID, st := range statuses {
		if st != domain.CarPublished {
			// pending / no_bot_access / too_large: blocks must be re-uploaded;
			// the recoverable markers are already set, nothing to change.
			continue
		}
		switch checks[carID] {
		case checkOK:
			// live published car — its blocks are deduplicated below
		case checkDeleted:
			plan.DeleteCars = append(plan.DeleteCars, carID)
			deleted[carID] = true
		case checkNoAccess:
			plan.SetStatus[carID] = domain.CarNoBotAccess
		case checkTooLarge:
			plan.SetStatus[carID] = domain.CarTooLarge
		}
	}

	for cidKey, ref := range existing {
		if statuses[ref.CarID] == domain.CarPublished &&
			checks[ref.CarID] == checkOK && !deleted[ref.CarID] {
			plan.Skip[cidKey] = true
		} else {
			plan.Reupload[cidKey] = true
		}
	}
	return plan
}
