// Package rendezvous implements rendezvous hashing (a.k.a. highest random
// weight hashing).
package rendezvous

import (
	"bytes"
	"cmp"
	"hash"
	"hash/crc32"
	"slices"
	"unsafe"
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// Hashable defines the requirements for a node type.
// It must provide a method to get its byte representation for hashing.
type Hashable interface {
	Bytes() []byte
}

// Hash implements rendezvous hashing for nodes of type N
// that satisfy the Hashable interface.
type Hash[N Hashable] struct {
	nodes  nodeScores[N]
	hasher hash.Hash32
}

// nodeScore holds a node and its calculated score for a given key.
type nodeScore[N Hashable] struct {
	node  N
	score uint32
}

// New returns a new Hash ready for use with the given nodes.
// N must satisfy the Hashable interface.
func New[N Hashable](nodes ...N) *Hash[N] {
	hash := &Hash[N]{
		hasher: crc32.New(crc32Table),
	}
	hash.Add(nodes...)
	return hash
}

func (h *Hash[N]) Add(nodes ...N) {
	for _, node := range nodes {
		h.nodes = append(h.nodes, nodeScore[N]{node: node})
	}
}

// Get returns the node with the highest score for the given key.
// If this Hash has no nodes, the zero value of type N is returned along with false.
func (h *Hash[N]) Get(key string) (N, bool) {
	if len(h.nodes) == 0 {
		var zero N
		return zero, false
	}

	keyBytes := unsafeBytes(key)

	maxNode := h.nodes[0].node
	maxScore := h.hash(maxNode, keyBytes)
	maxNodeBytes := maxNode.Bytes()

	for i := 1; i < len(h.nodes); i++ {
		currentNode := h.nodes[i].node
		score := h.hash(currentNode, keyBytes)

		if score > maxScore || (score == maxScore && bytes.Compare(currentNode.Bytes(), maxNodeBytes) < 0) {
			maxScore = score
			maxNode = currentNode
			maxNodeBytes = maxNode.Bytes()
		}
	}

	return maxNode, true
}

// GetN returns no more than n nodes for the given key, ordered by descending score.
func (h *Hash[N]) GetN(n int, key string) []N {
	if len(h.nodes) == 0 {
		return nil
	}
	keyBytes := unsafeBytes(key)
	for i := range h.nodes {
		h.nodes[i].score = h.hash(h.nodes[i].node, keyBytes)
	}

	slices.SortFunc(h.nodes, func(a, b nodeScore[N]) int {
		if b.score != a.score {
			return cmp.Compare(b.score, a.score)
		}
		return bytes.Compare(a.node.Bytes(), b.node.Bytes())
	})

	if n > len(h.nodes) {
		n = len(h.nodes)
	}

	nodes := make([]N, n)
	for i := range nodes {
		nodes[i] = h.nodes[i].node
	}
	return nodes
}

func (h *Hash[N]) Remove(node N) {
	nodeBytesToRemove := node.Bytes()
	h.nodes = slices.DeleteFunc(h.nodes, func(ns nodeScore[N]) bool {
		return bytes.Equal(ns.node.Bytes(), nodeBytesToRemove)
	})
}

// nodeScores is a slice of nodeScore structs.
type nodeScores[N Hashable] []nodeScore[N]

// hash generates the score using the node's HashBytes method and the key.
func (h *Hash[N]) hash(node N, key []byte) uint32 {
	h.hasher.Reset()
	h.hasher.Write(key)
	h.hasher.Write(node.Bytes())
	return h.hasher.Sum32()
}

// unsafeBytes converts string to byte slice without allocation.
func unsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
