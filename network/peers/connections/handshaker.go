package connections

import (
	"context"
	"github.com/bloxapp/ssv/network/peers"
	"github.com/bloxapp/ssv/network/records"
	"github.com/bloxapp/ssv/network/streams"
	forksprotocol "github.com/bloxapp/ssv/protocol/forks"
	libp2pnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"strings"
	"time"
)

const (
	// userAgentKey is the key used by libp2p to save user agent
	userAgentKey = "AgentVersion"
)

// errHandshakeInProcess is thrown when and handshake process for that peer is already running
var errHandshakeInProcess = errors.New("handshake already in process")

// errPeerWasFiltered is thrown when a peer is filtered during handshake
var errPeerWasFiltered = errors.New("peer was filtered during handshake")

// errUnknownUserAgent is thrown when a peer has an unknown user agent
var errUnknownUserAgent = errors.New("user agent is unknown")

// HandshakeFilter can be used to filter nodes once we handshaked with them
type HandshakeFilter func(info *records.NodeInfo) (bool, error)

// SubnetsProvider returns the subnets of or node
type SubnetsProvider func() records.Subnets

// Handshaker is the interface for handshaking with peers.
// it uses node info protocol to exchange information with other nodes and decide whether we want to connect.
//
// NOTE: due to compatibility with v0,
// we accept nodes with user agent as a fallback when the new protocol is not supported.
type Handshaker interface {
	Handshake(conn libp2pnetwork.Conn) error
	Handler() libp2pnetwork.StreamHandler
}

type handshaker struct {
	ctx context.Context

	logger *zap.Logger

	filters []HandshakeFilter

	streams     streams.StreamController
	nodeInfoIdx peers.NodeInfoIndex
	states      peers.NodeStates
	connIdx     peers.ConnectionIndex
	subnetsIdx  peers.SubnetsIndex
	ids         identify.IDService
	net         libp2pnetwork.Network

	subnetsProvider SubnetsProvider
}

// HandshakerCfg is the configuration for creating an handshaker instance
type HandshakerCfg struct {
	Logger          *zap.Logger
	Network         libp2pnetwork.Network
	Streams         streams.StreamController
	NodeInfoIdx     peers.NodeInfoIndex
	States          peers.NodeStates
	ConnIdx         peers.ConnectionIndex
	SubnetsIdx      peers.SubnetsIndex
	IDService       identify.IDService
	SubnetsProvider SubnetsProvider
}

// NewHandshaker creates a new instance of handshaker
func NewHandshaker(ctx context.Context, cfg *HandshakerCfg, filters ...HandshakeFilter) Handshaker {
	h := &handshaker{
		ctx:             ctx,
		logger:          cfg.Logger.With(zap.String("where", "Handshaker")),
		streams:         cfg.Streams,
		nodeInfoIdx:     cfg.NodeInfoIdx,
		connIdx:         cfg.ConnIdx,
		subnetsIdx:      cfg.SubnetsIdx,
		ids:             cfg.IDService,
		filters:         filters,
		states:          cfg.States,
		subnetsProvider: cfg.SubnetsProvider,
		net:             cfg.Network,
	}
	return h
}

// Handler returns the handshake handler
func (h *handshaker) Handler() libp2pnetwork.StreamHandler {
	return func(stream libp2pnetwork.Stream) {
		// start by marking the peer as pending
		pid := stream.Conn().RemotePeer()
		pidStr := pid.String()

		req, res, done, err := h.streams.HandleStream(stream)
		defer done()
		if err != nil {
			return
		}

		logger := h.logger.With(zap.String("otherPeer", pidStr))

		var ni records.NodeInfo
		err = ni.Consume(req)
		if err != nil {
			logger.Warn("could not consume node info request", zap.Error(err))
			return
		}
		// process the node info in a new goroutine so we won't block the stream
		go func() {
			err := h.processIncomingNodeInfo(pid, ni)
			if err != nil {
				if err == errPeerWasFiltered {
					logger.Debug("peer was filtered")
					return
				}
				logger.Warn("could not process node info", zap.Error(err))
			}
		}()

		self, err := h.nodeInfoIdx.SelfSealed()
		if err != nil {
			logger.Warn("could not seal self node info", zap.Error(err))
			return
		}
		if err := res(self); err != nil {
			logger.Warn("could not send self node info", zap.Error(err))
			return
		}
	}
}

func (h *handshaker) processIncomingNodeInfo(pid peer.ID, ni records.NodeInfo) error {
	h.updateNodeSubnets(pid, &ni)
	if !h.applyFilters(&ni) {
		return errPeerWasFiltered
	}
	if _, err := h.nodeInfoIdx.AddNodeInfo(pid, &ni); err != nil {
		return err
	}
	return nil
}

// preHandshake makes sure that we didn't reach peers limit and have exchanged framework information (libp2p)
// with the peer on the other side of the connection.
// it should enable us to know the supported protocols of peers we connect to
func (h *handshaker) preHandshake(conn libp2pnetwork.Conn) error {
	ctx, cancel := context.WithTimeout(h.ctx, time.Second*15)
	defer cancel()
	select {
	case <-ctx.Done():
		return errors.New("identity protocol (libp2p) timeout")
	case <-h.ids.IdentifyWait(conn):
	}
	return nil
}

// Handshake initiates handshake with the given conn
func (h *handshaker) Handshake(conn libp2pnetwork.Conn) error {
	pid := conn.RemotePeer()
	// check if the peer is known before we continue
	ni, err := h.getNodeInfo(pid)
	if err != nil || ni != nil {
		return err
	}
	//if err := h.preHandshake(conn); err != nil {
	//	return errors.Wrap(err, "could not perform pre-handshake")
	//}
	ni, err = h.nodeInfoFromStream(conn)
	if err != nil {
		// fallbacks to user agent
		ni, err = h.nodeInfoFromUserAgent(conn)
		if err != nil {
			return err
		}
	}
	if ni == nil {
		return errors.New("empty node info")
	}
	logger := h.logger.With(zap.String("otherPeer", pid.String()), zap.Any("info", ni))
	err = h.processIncomingNodeInfo(pid, *ni)
	if err != nil {
		logger.Debug("could not process node info", zap.Error(err))
		return err
	}

	return nil
}

func (h *handshaker) getNodeInfo(pid peer.ID) (*records.NodeInfo, error) {
	ni, err := h.nodeInfoIdx.GetNodeInfo(pid)
	if err != nil && err != peers.ErrNotFound {
		return nil, errors.Wrap(err, "could not read node info")
	}
	if ni != nil {
		switch h.states.State(pid) {
		case peers.StateIndexing:
			return nil, errHandshakeInProcess
		case peers.StatePruned:
			return nil, errors.Errorf("pruned peer [%s]", pid.String())
		case peers.StateReady:
			return ni, nil
		default: // unknown > continue the flow
		}
	}
	return nil, nil
}

// updateNodeSubnets tries to update the subnets of the given peer
func (h *handshaker) updateNodeSubnets(pid peer.ID, ni *records.NodeInfo) {
	if ni.Metadata != nil {
		subnets, err := records.Subnets{}.FromString(ni.Metadata.Subnets)
		if err == nil && len(subnets) > 0 {
			updated := h.subnetsIdx.UpdatePeerSubnets(pid, subnets)
			if updated {
				h.logger.Debug("[handshake] peer subnets were updated", zap.String("peerID", pid.String()),
					zap.String("subnets", subnets.String()))
			}
		}
	}
}

func (h *handshaker) nodeInfoFromStream(conn libp2pnetwork.Conn) (*records.NodeInfo, error) {
	res, err := h.net.Peerstore().FirstSupportedProtocol(conn.RemotePeer(), peers.NodeInfoProtocol)
	if err != nil {
		return nil, errors.Wrapf(err, "could not check supported protocols of peer %s",
			conn.RemotePeer().String())
	}
	data, err := h.nodeInfoIdx.SelfSealed()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.Errorf("peer [%s] doesn't supports handshake protocol", conn.RemotePeer().String())
	}
	resBytes, err := h.streams.Request(conn.RemotePeer(), peers.NodeInfoProtocol, data)
	if err != nil {
		return nil, err
	}
	var ni records.NodeInfo
	err = ni.Consume(resBytes)
	if err != nil {
		return nil, err
	}
	return &ni, nil
}

func (h *handshaker) nodeInfoFromUserAgent(conn libp2pnetwork.Conn) (*records.NodeInfo, error) {
	pid := conn.RemotePeer()
	uaRaw, err := h.net.Peerstore().Get(pid, userAgentKey)
	if err != nil {
		if err == peerstore.ErrNotFound {
			// if user agent wasn't found, retry libp2p identify after 100ms
			time.Sleep(time.Millisecond * 100)
			if err := h.preHandshake(conn); err != nil {
				return nil, err
			}
			uaRaw, err = h.net.Peerstore().Get(pid, userAgentKey)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	ua, ok := uaRaw.(string)
	if !ok {
		return nil, errors.New("could not cast ua to string")
	}
	parts := strings.Split(ua, ":")
	if len(parts) < 2 { // too old or unknown
		h.logger.Debug("user agent is unknown", zap.String("ua", ua))
		return nil, errUnknownUserAgent
	}
	// TODO: don't assume network is the same
	ni := records.NewNodeInfo(forksprotocol.GenesisForkVersion, h.nodeInfoIdx.Self().NetworkID)
	ni.Metadata = &records.NodeMetadata{
		NodeVersion: parts[1],
	}
	// extract operator id if exist
	if len(parts) > 3 {
		ni.Metadata.OperatorID = parts[3]
	}
	return ni, nil
}

func (h *handshaker) applyFilters(nodeInfo *records.NodeInfo) bool {
	for _, filter := range h.filters {
		ok, err := filter(nodeInfo)
		if err != nil {
			// h.logger.Warn("could not filter peer", zap.Error(err), zap.Any("nodeInfo", nodeInfo))
			return false
		}
		if !ok {
			return false
		}
	}
	return true
}
