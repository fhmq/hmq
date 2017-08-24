package broker

import (
	"errors"
	"sync"

	log "github.com/cihub/seelog"
)

// A result structure better optimized for queue subs.
type SublistResult struct {
	psubs []*subscription
	qsubs []*subscription // don't make this a map, too expensive to iterate
}

// A Sublist stores and efficiently retrieves subscriptions.
type Sublist struct {
	sync.RWMutex
	cache map[string]*SublistResult
	root  *level
}

// A node contains subscriptions and a pointer to the next level.
type node struct {
	next  *level
	psubs []*subscription
	qsubs []*subscription
}

// A level represents a group of nodes and special pointers to
// wildcard nodes.
type level struct {
	nodes map[string]*node
}

// Create a new default node.
func newNode() *node {
	return &node{psubs: make([]*subscription, 0, 4), qsubs: make([]*subscription, 0, 4)}
}

// Create a new default level. We use FNV1A as the hash
// algortihm for the tokens, which should be short.
func newLevel() *level {
	return &level{nodes: make(map[string]*node)}
}

// New will create a default sublist
func NewSublist() *Sublist {
	return &Sublist{root: newLevel(), cache: make(map[string]*SublistResult)}
}

// Insert adds a subscription into the sublist
func (s *Sublist) Insert(sub *subscription) error {

	tokens, err := SubscribeTopicCheckAndSpilt(sub.topic)
	if err != nil {
		return err
	}
	s.Lock()

	l := s.root
	var n *node
	for _, t := range tokens {
		n = l.nodes[t]
		if n == nil {
			n = newNode()
			l.nodes[t] = n
		}
		if n.next == nil {
			n.next = newLevel()
		}
		l = n.next
	}
	if sub.queue {
		//check qsub is already exist
		for i := range n.qsubs {
			if equal(n.qsubs[i], sub) {
				n.qsubs[i] = sub
				return nil
			}
		}
		n.qsubs = append(n.qsubs, sub)
	} else {
		//check psub is already exist
		for i := range n.psubs {
			if equal(n.psubs[i], sub) {
				n.psubs[i] = sub
				return nil
			}
		}
		n.psubs = append(n.psubs, sub)
	}

	topic := string(sub.topic)
	s.addToCache(topic, sub)
	s.Unlock()
	return nil
}

func (s *Sublist) addToCache(topic string, sub *subscription) {
	for k, r := range s.cache {
		if matchLiteral(k, topic) {
			// Copy since others may have a reference.
			nr := copyResult(r)
			if sub.queue == false {
				nr.psubs = append(nr.psubs, sub)
			} else {
				nr.qsubs = append(nr.qsubs, sub)
			}
			s.cache[k] = nr
		}
	}
}

func (s *Sublist) removeFromCache(topic string, sub *subscription) {
	for k := range s.cache {
		if !matchLiteral(k, topic) {
			continue
		}
		// Since someone else may be referecing, can't modify the list
		// safely, just let it re-populate.
		delete(s.cache, k)
	}
}

func matchLiteral(literal, topic string) bool {
	tok, _ := SubscribeTopicCheckAndSpilt([]byte(topic))
	li, _ := PublishTopicCheckAndSpilt([]byte(literal))

	for i := 0; i < len(tok); i++ {
		b := tok[i]
		switch b {
		case "+":

		case "#":
			return true
		default:
			if b != li[i] {
				return false
			}
		}
	}
	return true
}

// Deep copy
func copyResult(r *SublistResult) *SublistResult {
	nr := &SublistResult{}
	nr.psubs = append([]*subscription(nil), r.psubs...)
	nr.qsubs = append([]*subscription(nil), r.qsubs...)
	return nr
}

func (s *Sublist) Remove(sub *subscription) error {
	tokens, err := SubscribeTopicCheckAndSpilt(sub.topic)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()

	l := s.root
	var n *node

	for _, t := range tokens {
		if l == nil {
			return errors.New("No Matches subscription Found")
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if !s.removeFromNode(n, sub) {
		return errors.New("No Matches subscription Found")
	}
	topic := string(sub.topic)
	s.removeFromCache(topic, sub)
	return nil

}

func (s *Sublist) removeFromNode(n *node, sub *subscription) (found bool) {
	if n == nil {
		return false
	}

	if sub.queue {
		n.qsubs, found = removeSubFromList(sub, n.qsubs)
		return found
	} else {
		n.psubs, found = removeSubFromList(sub, n.psubs)
		return found
	}

	return false
}

func (s *Sublist) Match(topic string) *SublistResult {
	s.RLock()
	rc, ok := s.cache[topic]
	s.RUnlock()

	if ok {
		return rc
	}

	tokens, err := PublishTopicCheckAndSpilt([]byte(topic))
	if err != nil {
		log.Error("\tserver/sublist.go: ", err)
		return nil
	}

	result := &SublistResult{}

	s.Lock()
	l := s.root
	if len(tokens) > 0 {
		if tokens[0] == "/" {
			if _, exist := l.nodes["#"]; exist {
				addNodeToResults(l.nodes["#"], result)
			}
			if _, exist := l.nodes["+"]; exist {
				matchLevel(l.nodes["/"].next, tokens[1:], result)
			}
			if _, exist := l.nodes["/"]; exist {
				matchLevel(l.nodes["/"].next, tokens[1:], result)
			}
		} else {
			matchLevel(s.root, tokens, result)
		}
	}
	s.cache[topic] = result
	if len(s.cache) > 1024 {
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
	}

	s.Unlock()
	// log.Info("SublistResult: ", result)
	return result
}

func matchLevel(l *level, toks []string, results *SublistResult) {
	var swc, n *node
	exist := false
	for i, t := range toks {
		if l == nil {
			return
		}

		if _, exist = l.nodes["#"]; exist {
			addNodeToResults(l.nodes["#"], results)
		}
		if t != "/" {
			if swc, exist = l.nodes["+"]; exist {
				matchLevel(l.nodes["+"].next, toks[i+1:], results)
			}
		} else {
			if _, exist = l.nodes["+"]; exist {
				addNodeToResults(l.nodes["+"], results)
			}
		}

		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		addNodeToResults(n, results)
	}
	if swc != nil {
		addNodeToResults(n, results)
	}
}

// This will add in a node's results to the total results.
func addNodeToResults(n *node, results *SublistResult) {
	results.psubs = append(results.psubs, n.psubs...)
	results.qsubs = append(results.qsubs, n.qsubs...)
}

func removeSubFromList(sub *subscription, sl []*subscription) ([]*subscription, bool) {
	for i := 0; i < len(sl); i++ {
		if sl[i] == sub {
			last := len(sl) - 1
			sl[i] = sl[last]
			sl[last] = nil
			sl = sl[:last]
			// log.Info("removeSubFromList success")
			return shrinkAsNeeded(sl), true
		}
	}
	return sl, false
}

// Checks if we need to do a resize. This is for very large growth then
// subsequent return to a more normal size from unsubscribe.
func shrinkAsNeeded(sl []*subscription) []*subscription {
	lsl := len(sl)
	csl := cap(sl)
	// Don't bother if list not too big
	if csl <= 8 {
		return sl
	}
	pFree := float32(csl-lsl) / float32(csl)
	if pFree > 0.50 {
		return append([]*subscription(nil), sl...)
	}
	return sl
}
