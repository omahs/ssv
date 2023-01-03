package testing

import (
	"context"
	spectypes "github.com/bloxapp/ssv-spec/types"
	spectestingutils "github.com/bloxapp/ssv-spec/types/testingutils"
	"github.com/bloxapp/ssv/protocol/qbft/testing"
	"github.com/bloxapp/ssv/protocol/ssv/runner"
	validator2 "github.com/bloxapp/ssv/protocol/ssv/validator"
	"github.com/bloxapp/ssv/protocol/types"
)

var BaseValidator = func(keySet *spectestingutils.TestKeySet) *validator2.Validator {
	return validator2.NewValidator(
		context.TODO(),
		validator2.Options{
			Network: spectestingutils.NewTestingNetwork(),
			Beacon:  spectestingutils.NewTestingBeaconNode(),
			Storage: testing.TestingStores(),
			SSVShare: &types.SSVShare{
				Share: *spectestingutils.TestingShare(keySet),
			},
			Signer: spectestingutils.NewTestingKeyManager(),
			DutyRunners: map[spectypes.BeaconRole]runner.Runner{
				spectypes.BNRoleAttester:                  AttesterRunner(keySet),
				spectypes.BNRoleProposer:                  ProposerRunner(keySet),
				spectypes.BNRoleAggregator:                AggregatorRunner(keySet),
				spectypes.BNRoleSyncCommittee:             SyncCommitteeRunner(keySet),
				spectypes.BNRoleSyncCommitteeContribution: SyncCommitteeContributionRunner(keySet),
			},
		},
	)
}
