package eventstore

import "container/list"

// The AVL tree locates the first sequence after an arbitrary cursor. The list
// then walks a page without another tree lookup for each event.
type pendingQueue struct {
	items list.List
	root  *pendingNode
}

type pendingNode struct {
	sequence uint64
	item     *list.Element
	left     *pendingNode
	right    *pendingNode
	height   int
}

func pendingHeight(n *pendingNode) int {
	if n == nil {
		return 0
	}
	return n.height
}

func updatePendingHeight(n *pendingNode) {
	a, b := pendingHeight(n.left), pendingHeight(n.right)
	if a < b {
		a = b
	}
	n.height = a + 1
}

func rotatePendingLeft(n *pendingNode) *pendingNode {
	r := n.right
	n.right, r.left = r.left, n
	updatePendingHeight(n)
	updatePendingHeight(r)
	return r
}

func rotatePendingRight(n *pendingNode) *pendingNode {
	l := n.left
	n.left, l.right = l.right, n
	updatePendingHeight(n)
	updatePendingHeight(l)
	return l
}

func balancePending(n *pendingNode) *pendingNode {
	updatePendingHeight(n)
	if pendingHeight(n.left)-pendingHeight(n.right) > 1 {
		if pendingHeight(n.left.right) > pendingHeight(n.left.left) {
			n.left = rotatePendingLeft(n.left)
		}
		return rotatePendingRight(n)
	}
	if pendingHeight(n.right)-pendingHeight(n.left) > 1 {
		if pendingHeight(n.right.left) > pendingHeight(n.right.right) {
			n.right = rotatePendingRight(n.right)
		}
		return rotatePendingLeft(n)
	}
	return n
}

func insertPending(n *pendingNode, sequence uint64, item *list.Element) *pendingNode {
	if n == nil {
		return &pendingNode{sequence: sequence, item: item, height: 1}
	}
	if sequence < n.sequence {
		n.left = insertPending(n.left, sequence, item)
	} else {
		n.right = insertPending(n.right, sequence, item)
	}
	return balancePending(n)
}

func removePending(n *pendingNode, sequence uint64) *pendingNode {
	if n == nil {
		return nil
	}
	if sequence < n.sequence {
		n.left = removePending(n.left, sequence)
	} else if sequence > n.sequence {
		n.right = removePending(n.right, sequence)
	} else {
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		successor := n.right
		for successor.left != nil {
			successor = successor.left
		}
		n.sequence, n.item = successor.sequence, successor.item
		n.right = removePending(n.right, successor.sequence)
	}
	return balancePending(n)
}

func pendingAfter(n *pendingNode, cursor uint64) *list.Element {
	var result *list.Element
	for n != nil {
		if n.sequence > cursor {
			result = n.item
			n = n.left
		} else {
			n = n.right
		}
	}
	return result
}
