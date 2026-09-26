//go:build illumos || solaris

package downloader

import (
	"errors"
	"net"
)

// defaultGatewayIPv4 has no routing-table reader here (go-netroute does not
// support these platforms), so NAT traversal reports itself unavailable and
// the listen port can still be forwarded by hand.
func defaultGatewayIPv4() (net.IP, error) {
	return nil, errors.New("default gateway lookup is not supported on this platform")
}
