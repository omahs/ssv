package validator

import (
	"context"
	"encoding/hex"
	"github.com/bloxapp/ssv/protocol/v1/message"
	"time"

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	specqbft "github.com/bloxapp/ssv-spec/qbft"
	spectypes "github.com/bloxapp/ssv-spec/types"
	"github.com/bloxapp/ssv/protocol/v2/ssv/msgqueue"

	"github.com/patrickmn/go-cache"
	"go.uber.org/zap"
)

// GetLastHeight returns the last height for the given identifier
func (v *Validator) GetLastHeight(identifier spectypes.MessageID) specqbft.Height {
	r := v.DutyRunners.DutyRunnerForMsgID(identifier)
	if r == nil {
		return specqbft.Height(0)
	}
	state := r.GetBaseRunner().State
	if state == nil {
		return specqbft.Height(0)
	}
	return state.LastHeight
}

// GetLastSlot returns the last slot for the given identifier
func (v *Validator) GetLastSlot(identifier spectypes.MessageID) spec.Slot {
	r := v.DutyRunners.DutyRunnerForMsgID(identifier)
	if r == nil {
		return spec.Slot(0)
	}
	state := r.GetBaseRunner().State
	if state == nil {
		return spec.Slot(0)
	}
	return state.LastSlot
}

// MessageHandler process the msg. return error if exist
type MessageHandler func(msg *spectypes.SSVMessage) error

// StartQueueConsumer start ConsumeQueue with handler
func (v *Validator) StartQueueConsumer(msgId spectypes.MessageID, handler MessageHandler) {
	ctx, cancel := context.WithCancel(v.ctx)
	defer cancel()

	for ctx.Err() == nil {
		err := v.ConsumeQueue(msgId, handler, time.Millisecond*50)
		if err != nil {
			v.logger.Warn("could not consume queue", zap.Error(err))
		}
	}
}

// ConsumeQueue consumes messages from the msgqueue.Queue of the controller
// it checks for current state
func (v *Validator) ConsumeQueue(msgId spectypes.MessageID, handler MessageHandler, interval time.Duration) error {
	ctx, cancel := context.WithCancel(v.ctx)
	defer cancel()

	identifier := msgId.String()
	higherCache := cache.New(time.Second*12, time.Second*24)

	for ctx.Err() == nil {
		time.Sleep(interval)

		// no msg's in the queue
		if v.Q.Len() == 0 {
			// no msg's at all. need to prevent cpu usage in query
			time.Sleep(interval)
			continue
		}
		//// avoid process messages on fork
		//if atomic.LoadUint32(&v.State) == Forking {
		//	time.Sleep(interval)
		//	continue
		//}
		lastSlot := v.GetLastSlot(msgId)
		lastHeight := v.GetLastHeight(msgId)

		if processed := v.processHigherHeight(handler, identifier, lastHeight, higherCache); processed {
			v.logger.Debug("process higher height is done")
			continue
		}
		if !v.DutyRunners.DutyRunnerForMsgID(msgId).HasRunningDuty() {
			if processed := v.processNoRunningInstance(handler, identifier, lastHeight, lastSlot); processed {
				v.logger.Debug("process none running instance is done")
				continue
			}
		}
		if processed := v.processByState(handler, identifier, lastHeight); processed {
			v.logger.Debug("process by state is done")
			continue
		}
		if processed := v.processLateCommit(handler, identifier, lastHeight); processed {
			v.logger.Debug("process default is done")
			continue
		}

		// clean all old messages. (when stuck on change round stage, msgs not deleted)
		cleaned := v.Q.Clean(func(index msgqueue.Index) bool {
			oldHeight := index.H >= 0 && index.H <= (lastHeight-2) // remove all msg's that are 2 heights old. not post consensus & decided
			oldSlot := index.S > 0 && index.S < lastSlot
			return oldHeight || oldSlot
		})
		if cleaned > 0 {
			v.logger.Debug("indexes cleaned from queue", zap.Int64("count", cleaned))
		}
	}
	v.logger.Warn("queue consumer is closed")
	return nil
}

// processNoRunningInstance pop msg's only if no current instance running
func (v *Validator) processNoRunningInstance(
	handler MessageHandler,
	identifier string,
	lastHeight specqbft.Height,
	lastSlot spec.Slot,
) bool {
	//instance := v.GetCurrentInstance()
	//if instance != nil {
	//	return false // only pop when no instance running
	//}

	logger := v.logger.With(
		//zap.String("sig state", c.SignatureState.getState().toString()),
		zap.Int32("height", int32(lastHeight)),
		zap.Int32("slot", int32(lastSlot)))

	iterator := msgqueue.NewIndexIterator().Add(func() msgqueue.Index {
		return msgqueue.SignedPostConsensusMsgIndex(identifier, lastSlot)
	}, func() msgqueue.Index {
		return msgqueue.DecidedMsgIndex(identifier)
	}, func() msgqueue.Index {
		indices := msgqueue.SignedMsgIndex(spectypes.SSVConsensusMsgType, identifier, lastHeight, specqbft.CommitMsgType)
		if len(indices) == 0 {
			return msgqueue.Index{}
		}
		return indices[0]
	})

	msgs := v.Q.PopIndices(1, iterator)

	if len(msgs) == 0 || msgs[0] == nil {
		return false // no msg found
	}
	err := handler(msgs[0])
	if err != nil {
		logger.Warn("could not handle msg", zap.Error(err))
	}
	return true // msg processed
}

// processByState if an instance is running -> get the state and get the relevant messages
func (v *Validator) processByState(handler MessageHandler, identifier string, height specqbft.Height) bool {
	//currentInstance := v.GetCurrentInstance()
	//if currentInstance == nil {
	//	return false
	//}

	var msg *spectypes.SSVMessage

	//currentState := currentInstance.GetState()
	msg = v.getNextMsgForState(identifier, height)
	if msg == nil {
		return false // no msg found
	}
	v.logger.Debug("queue found message by state")
	//v.logger.Debug("queue found message for state",
	//	zap.Int32("stage", currentState.Stage.Load()),
	//	zap.Int32("seq", int32(currentState.GetHeight())),
	//	zap.Int32("round", int32(currentState.GetRound())),
	//)

	err := handler(msg)
	if err != nil {
		v.logger.Warn("could not handle msg", zap.Error(err))
	}
	return true // msg processed
}

// processHigherHeight fetch any message with higher height than last height
func (v *Validator) processHigherHeight(handler MessageHandler, identifier string, lastHeight specqbft.Height, higherCache *cache.Cache) bool {
	msgs := v.Q.WithIterator(1, true, func(index msgqueue.Index) bool {
		key := index.String()
		if _, found := higherCache.Get(key); !found {
			higherCache.Set(key, 0, cache.DefaultExpiration)
		} else {
			return false // skip msg
		}

		return index.ID == identifier && index.H > lastHeight
	})

	if len(msgs) > 0 {
		err := handler(msgs[0])
		if err != nil {
			v.logger.Warn("could not handle msg", zap.Error(err))
		}
		return true
	}
	return false
}

// processLateCommit this phase is to allow late commit and decided msg's
// we allow late commit and decided up to 1 height back. (only to support pre fork. after fork no need to support previews height)
func (v *Validator) processLateCommit(handler MessageHandler, identifier string, lastHeight specqbft.Height) bool {
	iterator := msgqueue.NewIndexIterator().
		Add(func() msgqueue.Index {
			indices := msgqueue.SignedMsgIndex(spectypes.SSVConsensusMsgType, identifier, lastHeight-1, specqbft.CommitMsgType)
			if len(indices) == 0 {
				return msgqueue.Index{}
			}
			return indices[0]
		}).Add(func() msgqueue.Index {
		indices := msgqueue.SignedMsgIndex(message.SSVDecidedMsgType, identifier, lastHeight-1, specqbft.CommitMsgType)
		if len(indices) == 0 {
			return msgqueue.Index{}
		}
		return indices[0]
	})
	msgs := v.Q.PopIndices(1, iterator)

	if len(msgs) > 0 {
		err := handler(msgs[0])
		if err != nil {
			v.logger.Warn("could not handle msg", zap.Error(err))
		}
		return true
	}
	return false
}

// getNextMsgForState return msgs depended on the current instance stage
func (v *Validator) getNextMsgForState(identifier string, height specqbft.Height) *spectypes.SSVMessage {
	iterator := msgqueue.NewIndexIterator()

	idxs := msgqueue.SignedMsgIndex(spectypes.SSVConsensusMsgType, identifier, height,
		specqbft.ProposalMsgType, specqbft.PrepareMsgType, specqbft.CommitMsgType, specqbft.RoundChangeMsgType)
	for _, idx := range idxs {
		iterator.AddIndex(idx)
	}

	iterator.
		Add(func() msgqueue.Index {
			return msgqueue.DecidedMsgIndex(identifier)
		}).
		Add(func() msgqueue.Index {
			indices := msgqueue.SignedMsgIndex(spectypes.SSVConsensusMsgType, identifier, height, specqbft.RoundChangeMsgType)
			if len(indices) == 0 {
				return msgqueue.Index{}
			}
			return indices[0]
		})

	msgs := v.Q.PopIndices(1, iterator)
	if len(msgs) == 0 {
		return nil
	}

	return msgs[0]
}

// processOnFork this phase is to allow process remaining decided messages that arrived late to the msg queue
func (v *Validator) processAllDecided(handler MessageHandler, identifier []byte) {
	idx := msgqueue.DecidedMsgIndex(hex.EncodeToString(identifier))
	msgs := v.Q.Pop(1, idx)
	for len(msgs) > 0 {
		err := handler(msgs[0])
		if err != nil {
			v.logger.Warn("could not handle msg", zap.Error(err))
		}
		msgs = v.Q.Pop(1, idx)
	}
}