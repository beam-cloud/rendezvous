// Package rendezvous implements rendezvous hashing (a.k.a. highest random
// weight hashing). See http://en.wikipedia.org/wiki/Rendezvous_hashing for
// more information.
package rendezvous

import (
	"cmp"
	"hash"
	"hash/crc32"
	"slices"
	"unsafe"
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// BytesFunc defines how a node N is converted to bytes for hashing.
type BytesFunc[N any] func(N) []byte

type Hash[N cmp.Ordered] struct {
	nodes     nodeScores[N]
	hasher    hash.Hash32
	bytesFunc BytesFunc[N]
}

type nodeScore[N cmp.Ordered] struct {
	node      N
	nodeBytes []byte
	score     uint32
}

// New returns a new Hash ready for use with the given nodes and byte conversion function.
// N must be an ordered type (implementing cmp.Ordered).
// bytesFunc specifies how to convert a node of type N to []byte for hashing.
func New[N cmp.Ordered](bytesFunc BytesFunc[N], nodes ...N) *Hash[N] {
	hash := &Hash[N]{
		hasher:    crc32.New(crc32Table),
		bytesFunc: bytesFunc,
	}
	hash.Add(nodes...)
	return hash
}

// Add adds additional nodes to the Hash.
func (h *Hash[N]) Add(nodes ...N) {
	for _, node := range nodes {
		nodeBytes := h.bytesFunc(node)
		h.nodes = append(h.nodes, nodeScore[N]{node: node, nodeBytes: nodeBytes})
	}
}

// Get returns the node with the highest score for the given key.
// If this Hash has no nodes, the zero value of type N is returned along with false.
func (h *Hash[N]) Get(key string) (N, bool) {
	if len(h.nodes) == 0 {
		var zero N
		return zero, false
	}

	var maxScore uint32
	maxNodeScore := h.nodes[0]

	keyBytes := unsafeBytes(key)

	// Calculate score for the first node
	maxNodeScore.score = h.hash(maxNodeScore.nodeBytes, keyBytes)
	maxScore = maxNodeScore.score

	// Iterate over remaining nodes
	for i := 1; i < len(h.nodes); i++ {
		nodeScore := h.nodes[i]
		score := h.hash(nodeScore.nodeBytes, keyBytes)

		if score > maxScore || (score == maxScore && cmp.Compare(nodeScore.node, maxNodeScore.node) < 0) {
			maxScore = score
			maxNodeScore = h.nodes[i]
		}
	}

	return maxNodeScore.node, true
}

// GetN returns no more than n nodes for the given key, ordered by descending score.
// GetN modifies the internal state for sorting and is not goroutine-safe.
func (h *Hash[N]) GetN(n int, key string) []N {
	if len(h.nodes) == 0 {
		return nil
	}
	keyBytes := unsafeBytes(key)
	for i := range h.nodes {
		h.nodes[i].score = h.hash(h.nodes[i].nodeBytes, keyBytes)
	}

	// Use slices.SortFunc with cmp.Compare for tie-breaking
	slices.SortFunc(h.nodes, func(a, b nodeScore[N]) int {
		if b.score != a.score {
			return cmp.Compare(b.score, a.score)
		}
		return cmp.Compare(a.node, b.node)
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

// Remove removes a node from the Hash, if it exists
func (h *Hash[N]) Remove(node N) {
	h.nodes = slices.DeleteFunc(h.nodes, func(ns nodeScore[N]) bool {
		return cmp.Compare(ns.node, node) == 0
	})
}

type nodeScores[N cmp.Ordered] []nodeScore[N]

// hash generates the score using pre-calculated node bytes and the key.
func (h *Hash[N]) hash(nodeBytes, key []byte) uint32 {
	h.hasher.Reset()
	h.hasher.Write(key)
	h.hasher.Write(nodeBytes) // Use pre-calculated bytes
	return h.hasher.Sum32()
}

// unsafeBytes converts string to byte slice without allocation.
// Requires Go 1.20+.
func unsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
