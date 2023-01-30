package validator

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/bloxapp/ssv/protocol/v2/message"

	specqbft "github.com/bloxapp/ssv-spec/qbft"
	specssv "github.com/bloxapp/ssv-spec/ssv"
	spectypes "github.com/bloxapp/ssv-spec/types"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/bloxapp/ssv/ibft/storage"
	"github.com/bloxapp/ssv/protocol/v2/ssv/queue"
	"github.com/bloxapp/ssv/protocol/v2/ssv/runner"
	"github.com/bloxapp/ssv/protocol/v2/types"
)

var logger = logging.Logger("ssv/protocol/ssv/validator").Desugar()

// Validator represents an SSV ETH consensus validator Share assigned, coordinates duty execution and more.
// Every validator has a validatorID which is validator's public key.
// Each validator has multiple DutyRunners, for each duty type.
type Validator struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	DutyRunners runner.DutyRunners
	Network     specqbft.Network
	Beacon      specssv.BeaconNode
	Share       *types.SSVShare
	Signer      spectypes.KeyManager

	Storage *storage.QBFTStores
	Queues  map[spectypes.BeaconRole]queueContainer

	state uint32
}

// NewValidator creates a new instance of Validator.
func NewValidator(pctx context.Context, options Options) *Validator {
	options.defaults()
	ctx, cancel := context.WithCancel(pctx)

	logger := logger.With(zap.String("validator", hex.EncodeToString(options.SSVShare.ValidatorPubKey)))

	logger.Debug("NIV: share", zap.Any("share", options.SSVShare.Share))

	v := &Validator{
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
		DutyRunners: options.DutyRunners,
		Network:     options.Network,
		Beacon:      options.Beacon,
		Storage:     options.Storage,
		Share:       options.SSVShare,
		Signer:      options.Signer,
		Queues:      make(map[spectypes.BeaconRole]queueContainer),
		state:       uint32(NotStarted),
	}

	for _, dutyRunner := range options.DutyRunners {
		// set timeout F
		dutyRunner.GetBaseRunner().TimeoutF = v.onTimeout
		v.Queues[dutyRunner.GetBaseRunner().BeaconRoleType] = queueContainer{
			Q: queue.New(),
			queueState: &queue.State{
				HasRunningInstance: false,
				Height:             0,
				Slot:               0,
				//Quorum:             options.SSVShare.Share,// TODO
			},
		}
	}

	return v
}

// StartDuty starts a duty for the validator
func (v *Validator) StartDuty(duty *spectypes.Duty) error {
	dutyRunner := v.DutyRunners[duty.Type]
	if dutyRunner == nil {
		return errors.Errorf("duty type %s not supported", duty.Type.String())
	}
	return dutyRunner.StartNewDuty(duty)
}

// ProcessMessage processes Network Message of all types
func (v *Validator) ProcessMessage(msg *queue.DecodedSSVMessage) error {
	start := time.Now()
	p2pID := msg.GetID().String()
	msgType := "<unknown>"
	var signers []spectypes.OperatorID

	msgTag := func() string {
		signersStr := ""
		for i, signer := range signers {
			if i != 0 {
				signersStr += "_"
			}
			signersStr += fmt.Sprint(signer)
		}
		return fmt.Sprintf("%s-%s-%s", msg.MsgID.GetRoleType().String(), msgType, signersStr)
	}
	logStart := func() {
		v.logger.Debug(
			fmt.Sprintf("started processing %s message", msgType),
			zap.String("p2p_id", p2pID),
			zap.String("tag", msgTag()),
		)
	}
	logEnd := func() {
		v.logger.Debug(
			fmt.Sprintf("done processing %s message", msgType),
			zap.String("p2p_id", p2pID),
			zap.String("tag", msgTag()),
			zap.Duration("took", time.Since(start)),
		)
	}

	dutyRunner := v.DutyRunners.DutyRunnerForMsgID(msg.GetID())
	if dutyRunner == nil {
		return errors.Errorf("could not get duty runner for msg ID")
	}

	if err := validateMessage(v.Share.Share, msg.SSVMessage); err != nil {
		return errors.Wrap(err, "Message invalid")
	}

	switch msg.GetType() {
	case spectypes.SSVConsensusMsgType:
		signedMsg, ok := msg.Body.(*specqbft.SignedMessage)
		if !ok {
			return errors.New("could not decode consensus message from network message")
		}
		signers = signedMsg.Signers
		switch signedMsg.Message.MsgType {
		case specqbft.PrepareMsgType:
			msgType = "prepare"
		case specqbft.CommitMsgType:
			msgType = "commit"
		case specqbft.ProposalMsgType:
			msgType = "proposal"
		case specqbft.RoundChangeMsgType:
			msgType = "round-change"
		default:
			msgType = fmt.Sprintf("<unknown-%d>", signedMsg.Message.MsgType)
		}
		logStart()
		defer logEnd()
		return dutyRunner.ProcessConsensus(signedMsg)
	case spectypes.SSVPartialSignatureMsgType:
		signedMsg, ok := msg.Body.(*specssv.SignedPartialSignatureMessage)
		if !ok {
			return errors.New("could not decode post consensus message from network message")
		}
		signers = signedMsg.GetSigners()
		if signedMsg.Message.Type == specssv.PostConsensusPartialSig {
			msgType = "post-consensus"
			return dutyRunner.ProcessPostConsensus(signedMsg)
		}
		msgType = "pre-consensus"
		logStart()
		defer logEnd()
		return dutyRunner.ProcessPreConsensus(signedMsg)
	case message.SSVEventMsgType:
		return v.handleEventMessage(msg, dutyRunner)
	default:
		return errors.New("unknown msg")
	}
}

func validateMessage(share spectypes.Share, msg *spectypes.SSVMessage) error {
	if !share.ValidatorPubKey.MessageIDBelongs(msg.GetID()) {
		return errors.New("msg ID doesn't match validator ID")
	}

	if len(msg.GetData()) == 0 {
		return errors.New("msg data is invalid")
	}

	return nil
}
