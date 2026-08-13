package dns

import (
	"context"
	"errors"

	D "github.com/miekg/dns"
	"github.com/slow-craft/nautilus-core/component/resolver"
	icontext "github.com/slow-craft/nautilus-core/context"
)

type Service struct {
	handler handler
}

// ServeMsg implement [resolver.Service] ResolveMsg
func (s *Service) ServeMsg(ctx context.Context, msg *D.Msg) (*D.Msg, error) {
	if len(msg.Question) == 0 {
		return nil, errors.New("at least one question is required")
	}

	return s.handler(icontext.NewDNSContext(ctx), msg)
}

var _ resolver.Service = (*Service)(nil)

func NewService(resolver resolver.Resolver, mapper *ResolverEnhancer) *Service {
	return &Service{handler: newHandler(resolver, mapper)}
}
