package queue

import (
	"github.com/bloxapp/ssv-spec/types"
	"sync/atomic"
)

// Filter is a function that returns true if the given message should be included.
type Filter func(*DecodedSSVMessage) bool

// Pop removes a message from the queue
type Pop func()

// FilterRole returns a Filter which returns true for messages whose BeaconRole matches the given role.
func FilterRole(role types.BeaconRole) Filter {
	return func(msg *DecodedSSVMessage) bool {
		return msg != nil && msg.MsgID.GetRoleType() == role
	}
}

// Queue objective is to receive messages and pop the right msg by to specify priority.
type Queue interface {
	// Push inserts a message to the queue
	Push(*DecodedSSVMessage)
	// Pop removes & returns the highest priority message which matches the given filter.
	// Returns nil if no message is found.
	Pop(MessagePrioritizer) *DecodedSSVMessage
	// IsEmpty checks if the q is empty
	IsEmpty() bool
}

// PriorityQueue implements Queue, it manages a lock-free linked list of DecodedSSVMessage.
// Implemented using atomic CAS (CompareAndSwap) operations to handle multiple goroutines that add/pop messages.
type PriorityQueue struct {
	head atomic.Pointer[msgItem]
}

// New initialized a PriorityQueue with the given MessagePrioritizer.
// If prioritizer is nil, the messages will be returned in the order they were pushed.
func New() Queue {
	pq := &PriorityQueue{}
	pq.head.Store(&msgItem{})
	return pq
}

func (q *PriorityQueue) Push(msg *DecodedSSVMessage) {
	n := &msgItem{}
	n.value.Store(msg)
	n.next.Store(q.head.Load())

	added := q.head.CompareAndSwap(q.head.Load(), n)
	if added {
		metricMsgQRatio.WithLabelValues(msg.MsgID.String()).Inc()
	}
}

func (q *PriorityQueue) Pop(prioritizer MessagePrioritizer) *DecodedSSVMessage {
	res := q.pop(prioritizer)
	if res != nil {
		metricMsgQRatio.WithLabelValues(res.MsgID.String()).Dec()
	}
	return res
}

func (q *PriorityQueue) IsEmpty() bool {
	h := q.head.Load()
	if h == nil {
		return true
	}
	return h.Value() == nil
}

func (q *PriorityQueue) pop(prioritizer MessagePrioritizer) *DecodedSSVMessage {
	var h, beforeHighest, previous *msgItem
	currentP := q.head.Load()

	for currentP != nil {
		current := currentP
		val := current.Value()
		if val != nil {
			if h == nil || prioritizer.Prior(val, h.Value()) {
				if previous != nil {
					beforeHighest = previous
				}
				h = current
			}
		}
		previous = current
		currentP = current.next.Load()
	}

	if h == nil {
		return nil
	}

	if beforeHighest != nil {
		beforeHighest.next.CompareAndSwap(beforeHighest.next.Load(), h.next.Load())
	} else {
		q.head.Store(h.next.Load())
	}

	return h.value.Load()
}

// msgItem is an item in the linked list that is used by PriorityQueue
type msgItem struct {
	value atomic.Pointer[DecodedSSVMessage]
	next  atomic.Pointer[msgItem]
}

// Value returns the underlaying value
func (i *msgItem) Value() *DecodedSSVMessage {
	return i.value.Load()
}
