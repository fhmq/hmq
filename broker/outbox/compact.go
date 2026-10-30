package outbox

import (
	"encoding/json"
	"errors"
)

// Compact removes acknowledged history while preserving the incarnation and
// next ID. It serializes with mutations and may require reopening on error.
// Snapshot memory is proportional to the pending payloads.
func (o *Outbox) Compact() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	replacer, ok := o.log.(interface{ Replace([][]byte) error })
	if !ok {
		return errors.New("outbox: log does not support replacement")
	}
	records := make([][]byte, 0, len(o.pending)+1)
	header, err := json.Marshal(record{Type: "peer", Peer: o.peer, Version: 2, Incarnation: o.incarnation, ID: o.next})
	if err != nil {
		return err
	}
	records = append(records, header)
	for _, id := range o.order {
		payload, exists := o.pending[id]
		if !exists {
			continue
		}
		data, err := json.Marshal(record{Type: "pending", ID: id, Payload: payload})
		if err != nil {
			return err
		}
		records = append(records, data)
	}
	if err := replacer.Replace(records); err != nil {
		o.failed = err
		return err
	}
	return nil
}
