package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	spectypes "github.com/bloxapp/ssv-spec/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/bloxapp/ssv/storage/basedb"
)

var (
	operatorsPrefix = []byte("operators")
)

// OperatorData the public data of an operator
type OperatorData struct {
	ID           spectypes.OperatorID `json:"id"`
	PublicKey    []byte               `json:"publicKey"`
	OwnerAddress common.Address       `json:"ownerAddress"`
}

// GetOperatorData is a function that returns the operator data
type GetOperatorData = func(index uint64) (*OperatorData, bool, error)

// OperatorsCollection is the interface for managing operators data
type OperatorsCollection interface {
	GetOperatorDataByPubKey(operatorPubKey []byte) (*OperatorData, bool, error)
	GetOperatorData(id spectypes.OperatorID) (*OperatorData, bool, error)
	SaveOperatorData(operatorData *OperatorData) error
	DeleteOperatorData(id spectypes.OperatorID) error
	ListOperators(from uint64, to uint64) ([]OperatorData, error)
	GetOperatorsPrefix() []byte
}

type operatorsStorage struct {
	db     basedb.IDb
	logger *zap.Logger
	lock   sync.RWMutex
	prefix []byte
}

// NewOperatorsStorage creates a new instance of Storage
func NewOperatorsStorage(db basedb.IDb, logger *zap.Logger, prefix []byte) OperatorsCollection {
	return &operatorsStorage{
		db:     db,
		logger: logger.With(zap.String("component", fmt.Sprintf("%sstorage", prefix))),
		prefix: prefix,
	}
}

// GetOperatorsPrefix returns the prefix
func (s *operatorsStorage) GetOperatorsPrefix() []byte {
	return operatorsPrefix
}

// ListOperators returns data of the all known operators by index range (from, to)
// when 'to' equals zero, all operators will be returned
func (s *operatorsStorage) ListOperators(from, to uint64) ([]OperatorData, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.listOperators(from, to)
}

// GetOperatorData returns data of the given operator by index
func (s *operatorsStorage) GetOperatorData(id spectypes.OperatorID) (*OperatorData, bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.getOperatorData(id)
}

// GetOperatorDataByPubKey returns data of the given operator by public key
func (s *operatorsStorage) GetOperatorDataByPubKey(operatorPubKey []byte) (*OperatorData, bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.getOperatorDataByPubKey(operatorPubKey)
}

func (s *operatorsStorage) getOperatorDataByPubKey(operatorPubKey []byte) (*OperatorData, bool, error) {
	operatorsData, err := s.listOperators(0, 0)
	if err != nil {
		return nil, false, errors.Wrap(err, "could not get all operators")
	}
	for _, op := range operatorsData {
		if bytes.Equal(op.PublicKey, operatorPubKey) {
			return &op, true, nil
		}
	}
	return nil, false, nil
}

func (s *operatorsStorage) getOperatorData(id spectypes.OperatorID) (*OperatorData, bool, error) {
	obj, found, err := s.db.Get(s.prefix, buildOperatorKey(id))
	if err != nil {
		return nil, found, err
	}
	if !found {
		return nil, found, nil
	}
	var operatorInformation OperatorData
	err = json.Unmarshal(obj.Value, &operatorInformation)
	return &operatorInformation, found, err
}

func (s *operatorsStorage) listOperators(from, to uint64) ([]OperatorData, error) {
	var operators []OperatorData
	err := s.db.GetAll(append(s.prefix, operatorsPrefix...), func(i int, obj basedb.Obj) error {
		var od OperatorData
		if err := json.Unmarshal(obj.Value, &od); err != nil {
			return err
		}
		if (uint64(od.ID) >= from && uint64(od.ID) <= to) || (to == 0) {
			operators = append(operators, od)
		}
		return nil
	})

	return operators, err
}

// SaveOperatorData saves operator data
func (s *operatorsStorage) SaveOperatorData(operatorData *OperatorData) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, found, err := s.getOperatorData(operatorData.ID)
	if err != nil {
		return errors.Wrap(err, "could not get operator data")
	}
	if found {
		s.logger.Debug("operator already exist",
			zap.String("pubKey", string(operatorData.PublicKey)),
			zap.Uint64("index", uint64(operatorData.ID)))
		return nil
	}

	raw, err := json.Marshal(operatorData)
	if err != nil {
		return errors.Wrap(err, "could not marshal operator data")
	}
	return s.db.Set(s.prefix, buildOperatorKey(operatorData.ID), raw)
}

func (s *operatorsStorage) DeleteOperatorData(id spectypes.OperatorID) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.db.Delete(s.prefix, buildOperatorKey(id))
}

// buildOperatorKey builds operator key using operatorsPrefix & index, e.g. "operators/1"
func buildOperatorKey(id spectypes.OperatorID) []byte {
	return bytes.Join([][]byte{operatorsPrefix, []byte(strconv.FormatUint(uint64(id), 10))}, []byte("/"))
}
