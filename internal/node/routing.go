// routing.go — delegated HTTP (IPNI) content routing, mirroring Kubo's
// Routing.DelegatedRouters: ["auto"]. It builds an HTTP delegated router
// pointing at https://delegated-ipfs.dev and combines it with the local
// Amino DHT so that BOTH providing (reprovide/Provide) and content lookups
// reach the delegated router (IPNI) in addition to the DHT. This is what makes
// freshly published content discoverable by public gateways quickly.

package node

import (
	"net/http"
	"time"

	httprouting "github.com/ipfs/boxo/routing/http/client"
	"github.com/ipfs/boxo/routing/http/contentrouter"
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
	// The HTTP client provides only ContentRouting; wrap it in a Compose so it
	// satisfies the full routing.Routing interface (nil PeerRouting/ValueStore
	// behave as the Null router).
	return &routinghelpers.Compose{
		ContentRouting: contentrouter.NewContentRoutingClient(c),
	}, nil
}

// combineRouters returns a routing.Routing that fans out to both the Amino DHT
// and the delegated HTTP router in parallel: Provide announces to both, and
// FindProviders queries both. The provider system uses this combined router so
// reprovides reach IPNI as well as the DHT. The DHT alone remains wired into
// libp2p's internal PeerRouting via the libp2p.Routing hook in node.New.
func combineRouters(kadDHT *dht.IpfsDHT, delegated routing.Routing) routing.Routing {
	return routinghelpers.Parallel{
		Routers:   []routing.Routing{kadDHT, delegated},
		Validator: record.NamespacedValidator{"pk": record.PublicKeyValidator{}},
	}
}
