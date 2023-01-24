package queue

import (
	"github.com/bloxapp/ssv-spec/types"
	"sync/atomic"
	"unsafe"
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
	head   unsafe.Pointer
	bucket unsafe.Pointer
}

// New initialized a PriorityQueue with the given MessagePrioritizer.
// If prioritizer is nil, the messages will be returned in the order they were pushed.
func New() Queue {
	// nolint
	h, b := unsafe.Pointer(&msgItem{}), unsafe.Pointer(&msgItem{})
	return &PriorityQueue{
		head:   h,
		bucket: b,
	}
}

func (q *PriorityQueue) Push(msg *DecodedSSVMessage) {
	// nolint
	n := &msgItem{value: unsafe.Pointer(msg), next: q.bucket}
	// nolint
	added := cas(&q.bucket, q.bucket, unsafe.Pointer(n))
	if added {
		metricMsgQRatio.WithLabelValues(msg.MsgID.String()).Inc()
	}
}

func (q *PriorityQueue) Pop(prioritizer MessagePrioritizer) *DecodedSSVMessage {
	q.flushBucket()
	res := q.pop(prioritizer)
	if res != nil {
		metricMsgQRatio.WithLabelValues(res.MsgID.String()).Dec()
	}
	return res
}

func (q *PriorityQueue) IsEmpty() bool {
	h := load(&q.head)
	if h != nil && h.Value() != nil {
		return false
	}
	b := load(&q.bucket)
	if b != nil && b.Value() != nil {
		return false
	}
	return true
}

// flushBucket moves the bucket list into head
func (q *PriorityQueue) flushBucket() {
	b := q.bucket
	current := load(&b)
	// nolint
	changed := cas(&q.bucket, b, unsafe.Pointer(&msgItem{}))
	// if swap failed we try to flush again (might be caused due to multiple goroutines trying to add messages)
	if !changed {
		q.flushBucket()
	}

	for current != nil {
		next := current.Next()
		if next == nil { // bucket is empty
			return
		}
		if next.Value() == nil { // reached end of bucket
			current.next = q.head
			// nolint
			_ = cas(&q.head, q.head, unsafe.Pointer(current))
			return
		}
		current = next
	}
}

func (q *PriorityQueue) pop(prioritizer MessagePrioritizer) *DecodedSSVMessage {
	var h, beforeHighest, previous *msgItem
	currentP := q.head

	for currentP != nil {
		current := load(&currentP)
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
		currentP = current.NextP()
	}

	if h == nil {
		return nil
	}

	if beforeHighest != nil {
		cas(&beforeHighest.next, beforeHighest.NextP(), h.NextP())
	} else {
		atomic.StorePointer(&q.head, h.NextP())
	}

	return h.Value()
}

func load(p *unsafe.Pointer) *msgItem {
	return (*msgItem)(atomic.LoadPointer(p))
}

func cas(p *unsafe.Pointer, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(p, old, new)
}

// msgItem is an item in the linked list that is used by PriorityQueue
type msgItem struct {
	value unsafe.Pointer //*DecodedSSVMessage
	next  unsafe.Pointer
}

// Value returns the underlaying value
func (i *msgItem) Value() *DecodedSSVMessage {
	return (*DecodedSSVMessage)(atomic.LoadPointer(&i.value))
}

// Next returns the next item in the list
func (i *msgItem) Next() *msgItem {
	return (*msgItem)(atomic.LoadPointer(&i.next))
}

// NextP returns the next item's pointer
func (i *msgItem) NextP() unsafe.Pointer {
	return atomic.LoadPointer(&i.next)
}
