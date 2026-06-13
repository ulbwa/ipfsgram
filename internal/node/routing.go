// routing.go — delegated HTTP (IPNI) content routing, mirroring Kubo's
// Routing.DelegatedRouters: ["auto"]. It builds an HTTP delegated router
// pointing at https://delegated-ipfs.dev and combines it with the local
// Amino DHT so that BOTH providing (reprovide/Provide) and content lookups
// reach the delegated router (IPNI) in addition to the DHT. This is what makes
// freshly published content discoverable by public gateways quickly.

package node

import (
	"context"
	"net/http"
	"time"

	httprouting "github.com/ipfs/boxo/routing/http/client"
	"github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	record "github.com/libp2p/go-libp2p-record"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/routing"
)

// delegatedRouterEndpoint is the "auto" HTTP delegated routing endpoint Kubo
// uses; it fronts the IPNI indexer network and the public DHT.
const delegatedRouterEndpoint = "https://delegated-ipfs.dev"

// newDelegatedRouter builds the HTTP delegated content router. It is a pure
// constructor (no network I/O), so a transient outage of the endpoint cannot
// fail node startup — lookups and provides against it simply error at call
// time and are logged by their callers.
func newDelegatedRouter() (routing.Routing, error) {
	c, err := httprouting.New(
		delegatedRouterEndpoint,
		httprouting.WithUserAgent("ipfsgram"),
		httprouting.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	)
	if err != nil {
		return nil, err
	}
	// Lookup-only. The HTTP delegated-routing write path (IPIP-526
	// ProvideBitswap) is deprecated and the public delegated-ipfs.dev endpoint is
	// read-only, so this router can only answer FindProviders, not announce. We
	// wrap it so Provide reports routing.ErrNotSupported: routinghelpers.Parallel
	// ignores ErrNotSupported (unlike a real error), so the combined router's
	// Provide/Reprovide succeeds via the DHT alone. Without this, every reprovide
	// fails on "cannot provide Bitswap records without an identity", which stops
	// provider records from ever being refreshed — so a node behind NAT never
	// republishes its freshly reserved relay /p2p-circuit address and stays
	// unreachable.
	//
	// The HTTP client provides only ContentRouting; wrap it in a Compose so it
	// satisfies the full routing.Routing interface (nil PeerRouting/ValueStore
	// behave as the Null router).
	return &routinghelpers.Compose{
		ContentRouting: lookupContentRouting{contentrouter.NewContentRoutingClient(c)},
	}, nil
}

// lookupContentRouting answers provider lookups via the embedded delegated
// content router but reports providing as unsupported, so the delegated router
// is never used to announce (see newDelegatedRouter).
type lookupContentRouting struct{ routing.ContentRouting }

func (lookupContentRouting) Provide(context.Context, cid.Cid, bool) error {
	return routing.ErrNotSupported
}

// combineRouters returns a routing.Routing that fans out to both the Amino DHT
// and the delegated HTTP router in parallel: FindProviders queries both, while
// Provide reaches only the DHT (the delegated router reports providing as
// unsupported — see newDelegatedRouter). The provider system uses this combined
// router so lookups can hit IPNI while announces go to the DHT. The DHT alone
// remains wired into libp2p's internal PeerRouting via the libp2p.Routing hook
// in node.New.
func combineRouters(kadDHT *dht.IpfsDHT, delegated routing.Routing) routing.Routing {
	return routinghelpers.Parallel{
		Routers:   []routing.Routing{kadDHT, delegated},
		Validator: record.NamespacedValidator{"pk": record.PublicKeyValidator{}},
	}
}
