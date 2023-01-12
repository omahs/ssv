package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	specqbft "github.com/bloxapp/ssv-spec/qbft"
	spectypes "github.com/bloxapp/ssv-spec/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/bloxapp/ssv/protocol/v2/qbft"
	"github.com/bloxapp/ssv/protocol/v2/qbft/instance"
	logging "github.com/ipfs/go-log"
)

var logger = logging.Logger("ssv/protocol/qbft/controller").Desugar()

// Controller is a QBFT coordinator responsible for starting and following the entire life cycle of multiple QBFT InstanceContainer
type Controller struct {
	Identifier []byte
	Height     specqbft.Height // incremental Height for InstanceContainer
	// StoredInstances stores the last HistoricalInstanceCapacity in an array for message processing purposes.
	StoredInstances InstanceContainer
	// FutureMsgsContainer holds all msgs from a higher height
	FutureMsgsContainer map[spectypes.OperatorID]specqbft.Height // maps msg signer to height of higher height received msgs
	Domain              spectypes.DomainType
	Share               *spectypes.Share
	config              qbft.IConfig
	logger              *zap.Logger
}

func NewController(
	identifier []byte,
	share *spectypes.Share,
	domain spectypes.DomainType,
	config qbft.IConfig,
) *Controller {
	return &Controller{
		Identifier:          identifier,
		Height:              specqbft.FirstHeight,
		Domain:              domain,
		Share:               share,
		StoredInstances:     InstanceContainer{},
		FutureMsgsContainer: make(map[spectypes.OperatorID]specqbft.Height),
		config:              config,
		logger:              logger.With(zap.String("identifier", spectypes.MessageIDFromBytes(identifier).String())),
	}
}

// StartNewInstance will start a new QBFT instance, if can't will return error
func (c *Controller) StartNewInstance(value []byte) error {
	if err := c.canStartInstanceForValue(value); err != nil {
		return errors.Wrap(err, "can't start new QBFT instance")
	}

	// only if current height's instance exists (and decided since passed can start instance) bump
	if c.StoredInstances.FindInstance(c.Height) != nil {
		c.bumpHeight()
	}

	newInstance := c.addAndStoreNewInstance()
	newInstance.Start(value, c.Height)

	return nil
}

// ProcessMsg processes a new msg, returns decided message or error
func (c *Controller) ProcessMsg(msg *specqbft.SignedMessage) (*specqbft.SignedMessage, error) {
	if err := c.baseMsgValidation(msg); err != nil {
		return nil, errors.Wrap(err, "invalid msg")
	}

	/**
	Main controller processing flow
	_______________________________
	All decided msgs are processed the same, out of instance
	All valid future msgs are saved in a container and can trigger highest decided futuremsg
	All other msgs (not future or decided) are processed normally by an existing instance (if found)
	*/
	if isDecidedMsg(c.Share, msg) {
		return c.UponDecided(msg)
	} else if msg.Message.Height > c.Height {
		return c.UponFutureMsg(msg)
	} else {
		return c.UponExistingInstanceMsg(msg)
	}
}

func (c *Controller) UponExistingInstanceMsg(msg *specqbft.SignedMessage) (*specqbft.SignedMessage, error) {
	inst := c.InstanceForHeight(msg.Message.Height)
	if inst == nil {
		return nil, errors.New("instance not found")
	}

	prevDecided, _ := inst.IsDecided()

	decided, _, decidedMsg, err := inst.ProcessMsg(msg)
	if err != nil {
		return nil, errors.Wrap(err, "could not process msg")
	}

	// if previously Decided we do not return Decided true again
	if prevDecided {
		return nil, err
	}

	// save the highest Decided
	if !decided {
		return nil, nil
	}

	if err := c.broadcastDecided(decidedMsg); err != nil {
		// no need to fail processing instance deciding if failed to save/ broadcast
		c.logger.Debug("failed to broadcast decided message", zap.Error(err))
	}
	return msg, nil
}

// BaseMsgValidation returns error if msg is invalid (base validation)
func (c *Controller) baseMsgValidation(msg *specqbft.SignedMessage) error {
	if msg == nil || msg.Message == nil {
		return errors.New("empty message")
	}
	// verify msg belongs to controller
	if !bytes.Equal(c.Identifier, msg.Message.Identifier) {
		return errors.New("message doesn't belong to Identifier")
	}

	return nil
}

func (c *Controller) InstanceForHeight(height specqbft.Height) *instance.Instance {
	return c.StoredInstances.FindInstance(height)
}

func (c *Controller) bumpHeight() {
	c.Height++
}

// GetIdentifier returns QBFT Identifier, used to identify messages
func (c *Controller) GetIdentifier() []byte {
	return c.Identifier
}

// addAndStoreNewInstance returns creates a new QBFT instance, stores it in an array and returns it
func (c *Controller) addAndStoreNewInstance() *instance.Instance {
	i := instance.NewInstance(c.GetConfig(), c.Share, c.Identifier, c.Height)
	c.StoredInstances.addNewInstance(i)
	return i
}

func (c *Controller) canStartInstanceForValue(value []byte) error {
	// check value
	if err := c.GetConfig().GetValueCheckF()(value); err != nil {
		return errors.Wrap(err, "value invalid")
	}

	return c.CanStartInstance()
}

// CanStartInstance returns nil if controller can start a new instance
func (c *Controller) CanStartInstance() error {
	// check prev instance if prev instance is not the first instance
	inst := c.StoredInstances.FindInstance(c.Height)
	if inst == nil {
		return nil
	}
	if decided, _ := inst.IsDecided(); !decided {
		return errors.New("previous instance hasn't Decided")
	}

	return nil
}

// GetRoot returns the state's deterministic root
func (c *Controller) GetRoot() ([]byte, error) {
	marshaledRoot, err := json.Marshal(c)
	if err != nil {
		return nil, errors.Wrap(err, "could not encode state")
	}
	ret := sha256.Sum256(marshaledRoot)
	return ret[:], nil
}

// Encode implementation
func (c *Controller) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// Decode implementation
func (c *Controller) Decode(data []byte) error {
	err := json.Unmarshal(data, &c)
	if err != nil {
		return errors.Wrap(err, "could not decode controller")
	}

	config := c.GetConfig()
	for _, i := range c.StoredInstances {
		if i != nil {
			// TODO-spec-align changed due to instance and controller are not in same package as in spec, do we still need it for test?
			i.SetConfig(config)
		}
	}
	return nil
}

func (c *Controller) broadcastDecided(aggregatedCommit *specqbft.SignedMessage) error {
	// Broadcast Decided msg
	byts, err := aggregatedCommit.Encode()
	if err != nil {
		return errors.Wrap(err, "could not encode decided message")
	}

	msgToBroadcast := &spectypes.SSVMessage{
		MsgType: spectypes.SSVConsensusMsgType,
		MsgID:   specqbft.ControllerIdToMessageID(c.Identifier),
		Data:    byts,
	}
	if err := c.GetConfig().GetNetwork().Broadcast(msgToBroadcast); err != nil {
		// We do not return error here, just Log broadcasting error.
		return errors.Wrap(err, "could not broadcast decided")
	}
	return nil
}

func (c *Controller) GetConfig() qbft.IConfig {
	return c.config
}
