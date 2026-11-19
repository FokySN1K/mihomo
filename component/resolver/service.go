package resolver

import (
	"context"

	"github.com/metacubex/mihomo/common/atomic"

	D "github.com/miekg/dns"
)

// service is read on the DNS path while an apply may replace it.
var service atomic.TypedValue[Service]

// SetService installs the DNS service an apply built.
func SetService(s Service) {
	service.Store(s)
}

type Service interface {
	ServeMsg(ctx context.Context, msg *D.Msg) (*D.Msg, error)
}

// ServeMsg with a dns.Msg, return resolve dns.Msg
func ServeMsg(ctx context.Context, msg *D.Msg) (*D.Msg, error) {
	if server, _ := service.LoadOk(); server != nil {
		return server.ServeMsg(ctx, msg)
	}

	return nil, ErrIPNotFound
}
