package syncing

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bloxapp/ssv-spec/qbft"
	specqbft "github.com/bloxapp/ssv-spec/qbft"
	spectypes "github.com/bloxapp/ssv-spec/types"
	protocolp2p "github.com/bloxapp/ssv/protocol/v2/p2p"
	"github.com/bloxapp/ssv/utils/tasks"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

//go:generate mockgen -package=mocks -destination=./mocks/syncer.go -source=./syncer.go

// MessageHandler reacts to a message received from Syncer.
type MessageHandler func(msg spectypes.SSVMessage)

// Throttle returns a MessageHandler that throttles the given handler.
func Throttle(handler MessageHandler, throttle time.Duration) MessageHandler {
	var last atomic.Pointer[time.Time]
	now := time.Now()
	last.Store(&now)

	return func(msg spectypes.SSVMessage) {
		delta := time.Since(*last.Load())
		if delta < throttle {
			time.Sleep(throttle - delta)
		}

		now := time.Now()
		last.Store(&now)

		handler(msg)
	}
}

// Syncer handles the syncing of decided messages.
type Syncer interface {
	SyncHighestDecided(ctx context.Context, id spectypes.MessageID, handler MessageHandler) error
	SyncDecidedByRange(
		ctx context.Context,
		id spectypes.MessageID,
		from, to specqbft.Height,
		handler MessageHandler,
	) error
}

// Network is a subset of protocolp2p.Syncer, required by Syncer to retrieve messages from peers.
type Network interface {
	LastDecided(id spectypes.MessageID) ([]protocolp2p.SyncResult, error)
	GetHistory(
		id spectypes.MessageID,
		from, to specqbft.Height,
		targets ...string,
	) ([]protocolp2p.SyncResult, specqbft.Height, error)
}

type syncer struct {
	network Network
	logger  *zap.Logger
}

// New returns a standard implementation of Syncer.
func New(logger *zap.Logger, network Network) Syncer {
	return &syncer{
		logger:  logger.With(zap.String("who", "Syncer")),
		network: network,
	}
}

func (s *syncer) SyncHighestDecided(
	ctx context.Context,
	id spectypes.MessageID,
	handler MessageHandler,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	logger := s.logger.With(
		zap.String("what", "SyncHighestDecided"),
		zap.String("publicKey", hex.EncodeToString(id.GetPubKey())),
		zap.String("role", id.GetRoleType().String()))

	lastDecided, err := s.network.LastDecided(id)
	if err != nil {
		logger.Debug("sync failed", zap.Error(err))
		return errors.Wrap(err, "could not sync last decided")
	}
	if len(lastDecided) == 0 {
		logger.Debug("no messages were synced")
		return nil
	}

	results := protocolp2p.SyncResults(lastDecided)
	results.ForEachSignedMessage(func(m *specqbft.SignedMessage) (stop bool) {
		if ctx.Err() != nil {
			return true
		}
		raw, err := m.Encode()
		if err != nil {
			logger.Debug("could not encode signed message", zap.Error(err))
			return false
		}
		handler(spectypes.SSVMessage{
			MsgType: spectypes.SSVConsensusMsgType,
			MsgID:   id,
			Data:    raw,
		})
		return false
	})
	return nil
}

func (s *syncer) SyncDecidedByRange(
	ctx context.Context,
	id spectypes.MessageID,
	from, to qbft.Height,
	handler MessageHandler,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	logger := s.logger.With(
		zap.String("what", "SyncDecidedByRange"),
		zap.String("publicKey", hex.EncodeToString(id.GetPubKey())),
		zap.String("role", id.GetRoleType().String()),
		zap.Uint64("from", uint64(from)),
		zap.Uint64("to", uint64(to)))
	logger.Debug("syncing decided by range")

	err := s.getDecidedByRange(
		context.Background(),
		logger,
		id,
		from,
		to,
		func(sm *specqbft.SignedMessage) error {
			raw, err := sm.Encode()
			if err != nil {
				logger.Debug("could not encode signed message", zap.Error(err))
				return nil
			}
			handler(spectypes.SSVMessage{
				MsgType: spectypes.SSVConsensusMsgType,
				MsgID:   id,
				Data:    raw,
			})
			return nil
		},
	)
	if err != nil {
		logger.Debug("sync failed", zap.Error(err))
	}
	return err
}

// getDecidedByRange calls GetHistory in batches to retrieve all decided messages in the given range.
func (s *syncer) getDecidedByRange(
	ctx context.Context,
	logger *zap.Logger,
	mid spectypes.MessageID,
	from, to specqbft.Height,
	handler func(*specqbft.SignedMessage) error,
) error {
	const maxRetries = 2

	var (
		visited = make(map[specqbft.Height]struct{})
		msgs    []protocolp2p.SyncResult
	)

	tail := from
	var err error
	for tail < to {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := tasks.RetryWithContext(ctx, func() error {
			start := time.Now()
			msgs, tail, err = s.network.GetHistory(mid, tail, to)
			if err != nil {
				return err
			}
			handled := 0
			protocolp2p.SyncResults(msgs).ForEachSignedMessage(func(m *specqbft.SignedMessage) (stop bool) {
				if ctx.Err() != nil {
					return true
				}
				if _, ok := visited[m.Message.Height]; ok {
					return false
				}
				if err := handler(m); err != nil {
					logger.Warn("could not handle signed message")
				}
				handled++
				visited[m.Message.Height] = struct{}{}
				return false
			})
			logger.Debug("received and processed history batch",
				zap.Int64("tail", int64(tail)),
				zap.Duration("duration", time.Since(start)),
				zap.Int("results_count", len(msgs)),
				zap.String("results", s.formatSyncResults(msgs)),
				zap.Int("handled", handled))
			return nil
		}, maxRetries)
		if err != nil {
			return err
		}
	}

	return nil
}

// Prints msgs as:
//
//	"(type=1 height=1 round=1) (type=1 height=2 round=1) ..."
//
// TODO: remove this after testing?
func (s *syncer) formatSyncResults(msgs []protocolp2p.SyncResult) string {
	var v []string
	for _, m := range msgs {
		var sm *specqbft.SignedMessage
		if m.Msg.MsgType == spectypes.SSVConsensusMsgType {
			sm = &specqbft.SignedMessage{}
			if err := sm.Decode(m.Msg.Data); err != nil {
				v = append(v, fmt.Sprintf("(%v)", err))
				continue
			}
			v = append(
				v,
				fmt.Sprintf(
					"(type=%d height=%d round=%d)",
					m.Msg.MsgType,
					sm.Message.Height,
					sm.Message.Round,
				),
			)
		}
		v = append(v, fmt.Sprintf("(type=%d)", m.Msg.MsgType))
	}
	return strings.Join(v, ", ")
}
