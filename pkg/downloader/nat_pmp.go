package downloader

import (
	"context"
	"errors"
	"net"
	"time"

	natpmp "github.com/jackpal/go-nat-pmp"
)

// natpmpClient is what natpmpMapper needs from go-nat-pmp. Its calls take no
// context: the client's timeout bounds them, and callWithContext lets callers
// stop waiting early.
type natpmpClient interface {
	GetExternalAddress() (*natpmp.GetExternalAddressResult, error)
	AddPortMapping(protocol string, internalPort, requestedExternalPort, lifetime int) (*natpmp.AddPortMappingResult, error)
}

func newNATPMPClient(gateway net.IP) natpmpClient {
	// Without a timeout the library retries for about 128 seconds.
	return natpmp.NewClientWithTimeout(gateway, natOperationTimeout)
}

// natpmpMapper maps ports with NAT-PMP (RFC 6886) on the default gateway.
type natpmpMapper struct {
	client natpmpClient
}

func (n *natpmpMapper) Type() string { return "NAT-PMP" }

func (n *natpmpMapper) GetExternalAddress(ctx context.Context) (net.IP, error) {
	res, err := callWithContext(ctx, n.client.GetExternalAddress)
	if err != nil {
		return nil, err
	}
	ip := res.ExternalIPAddress
	return net.IPv4(ip[0], ip[1], ip[2], ip[3]), nil
}

func (n *natpmpMapper) AddPortMapping(ctx context.Context, protocol string, internalPort, externalPort int,
	_ string, lifetime time.Duration) (int, error) {
	if !validNATPort(externalPort) {
		externalPort = randomNATPort()
	}
	seconds := int(lifetime / time.Second)
	res, err := callWithContext(ctx, func() (*natpmp.AddPortMappingResult, error) {
		return n.client.AddPortMapping(protocol, internalPort, externalPort, seconds)
	})
	if err != nil {
		return 0, err
	}
	// The requested port is only a hint (RFC 6886 section 3.3); the gateway
	// may grant another, and that is the one peers can reach.
	if res.MappedExternalPort == 0 {
		return 0, errors.New("NAT-PMP gateway granted no external port")
	}
	return int(res.MappedExternalPort), nil
}

func (n *natpmpMapper) DeletePortMapping(ctx context.Context, protocol string, internalPort, _ int) error {
	// RFC 6886 section 3.4: a request with lifetime 0 and external port 0
	// deletes the mapping for internalPort.
	_, err := callWithContext(ctx, func() (*natpmp.AddPortMappingResult, error) {
		return n.client.AddPortMapping(protocol, internalPort, 0, 0)
	})
	return err
}
