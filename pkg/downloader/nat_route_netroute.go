//go:build !illumos && !solaris

package downloader

import (
	"errors"
	"net"

	"github.com/libp2p/go-netroute"
)

// natRouteProbe is a public (TEST-NET-3) destination whose route is the
// default route. go-netroute refuses to route 0.0.0.0 itself on Linux.
var natRouteProbe = net.IPv4(203, 0, 113, 1)

func defaultGatewayIPv4() (net.IP, error) {
	router, err := netroute.New()
	if err != nil {
		return nil, err
	}
	_, gateway, _, err := router.Route(natRouteProbe)
	if err != nil {
		return nil, err
	}
	if gateway = gateway.To4(); gateway == nil || gateway.IsUnspecified() {
		return nil, errors.New("no IPv4 default gateway")
	}
	return gateway, nil
}
