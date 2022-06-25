package lightnode

import (
	"context"
	"github.com/bloxapp/ssv/protocol/v1/message"
	p2pprotocol "github.com/bloxapp/ssv/protocol/v1/p2p"
	"github.com/bloxapp/ssv/protocol/v1/qbft/pipelines"
	qbftstorage "github.com/bloxapp/ssv/protocol/v1/qbft/storage"
	"github.com/bloxapp/ssv/protocol/v1/qbft/strategy"
	"github.com/bloxapp/ssv/protocol/v1/sync/lastdecided"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// lightNode implements strategy.Decided
type lightNode struct {
	logger         *zap.Logger
	store          qbftstorage.DecidedMsgStore
	decidedFetcher lastdecided.Fetcher
}

// NewLightNodeStrategy creates a new instance of light node strategy
func NewLightNodeStrategy(logger *zap.Logger, store qbftstorage.DecidedMsgStore, syncer p2pprotocol.Syncer) strategy.Decided {
	return &lightNode{
		logger:         logger.With(zap.String("who", "LightNodeStrategy")),
		store:          store,
		decidedFetcher: lastdecided.NewLastDecidedFetcher(logger, syncer),
	}
}

func (ln *lightNode) Sync(ctx context.Context, identifier message.Identifier, from, to *message.SignedMessage, pip pipelines.SignedMessagePipeline) error {
	if to == nil {
		highest, _, _, err := ln.decidedFetcher.GetLastDecided(ctx, identifier, func(i message.Identifier) (*message.SignedMessage, error) {
			return from, nil
		})
		if err != nil {
			return errors.Wrap(err, "could not get last decided from peers")
		}
		to = highest
	}
	if to != nil {
		_, err := ln.UpdateDecided(to)
		return errors.Wrap(err, "could not save decided")
	}
	return nil
}

func (ln *lightNode) UpdateDecided(msg *message.SignedMessage) (*message.SignedMessage, error) {
	return strategy.UpdateLastDecided(ln.logger, ln.store, msg)
}

// GetDecided in light node will try to look for last decided and returns it if in the given range
func (ln *lightNode) GetDecided(identifier message.Identifier, heightRange ...message.Height) ([]*message.SignedMessage, error) {
	if len(heightRange) < 2 {
		return nil, errors.New("missing height range")
	}
	ld, err := ln.store.GetLastDecided(identifier)
	if err != nil {
		return nil, err
	}
	if ld == nil {
		return nil, nil
	}
	height, from, to := ld.Message.Height, heightRange[0], heightRange[1]
	if height < from || height > to {
		return nil, nil
	}
	return []*message.SignedMessage{ld}, nil
}

func (ln *lightNode) GetLastDecided(identifier message.Identifier) (*message.SignedMessage, error) {
	return ln.store.GetLastDecided(identifier)
}