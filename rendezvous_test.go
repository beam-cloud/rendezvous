package rendezvous

import (
	"fmt"
	"reflect"
	"testing"
)

// hashableString implements HashableOrdered for testing purposes.
type hashableString string

// Bytes implements the HashableOrdered interface.
func (hs hashableString) Bytes() []byte {
	return []byte(hs)
}

var sampleKeys = []string{
	"352DAB08-C1FD-4462-B573-7640B730B721",
	"382080D3-B847-4BB5-AEA8-644C3E56F4E1",
	"2B340C12-7958-4DBE-952C-67496E15D0C8",
	"BE05F82B-902E-4868-8CC9-EE50A6C64636",
	"C7ECC571-E924-4523-A313-951DFD5D8073",
}

// Use hashableString instead of a generic comparable type here for simplicity in tests
type getTestcase struct {
	key          string
	expectedNode hashableString
	expectOk     bool
}

func TestHashGet(t *testing.T) {
	hash := New[hashableString]()

	gotNode, ok := hash.Get("foo")
	if ok || gotNode != "" {
		t.Errorf("got: (%v, %t), expected: (%v, false)", gotNode, ok, hashableString(""))
	}

	nodes := []hashableString{"a", "b", "c", "d", "e"}
	hash.Add(nodes...)

	testcases := []getTestcase{
		{"", "d", true},
		{"foo", "e", true},
		{"bar", "c", true},
	}

	for _, testcase := range testcases {
		gotNode, ok := hash.Get(testcase.key)
		if ok != testcase.expectOk || gotNode != testcase.expectedNode {
			t.Errorf("key=%q - got: (%v, %t), expected: (%v, %t)", testcase.key, gotNode, ok, testcase.expectedNode, testcase.expectOk)
		}
	}
}

func BenchmarkHashGet_5nodes(b *testing.B) {
	hash := New(hashableString("a"), hashableString("b"), hashableString("c"), hashableString("d"), hashableString("e"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.Get(sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkHashGet_10nodes(b *testing.B) {
	hash := New(hashableString("a"), hashableString("b"), hashableString("c"), hashableString("d"), hashableString("e"), hashableString("f"), hashableString("g"), hashableString("h"), hashableString("i"), hashableString("j"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.Get(sampleKeys[i%len(sampleKeys)])
	}
}

// Use hashableString instead of generic comparable
type getNTestcase struct {
	count         int
	key           string
	expectedNodes []hashableString
}

func Test_Hash_GetN(t *testing.T) {
	hash := New[hashableString]()

	gotNodes := hash.GetN(2, "foo")
	if len(gotNodes) != 0 {
		t.Errorf("got: %#v, expected: %#v", gotNodes, []hashableString{})
	}

	hash.Add("a", "b", "c", "d", "e")

	testcases := []getNTestcase{
		{1, "foo", []hashableString{"e"}},
		{2, "bar", []hashableString{"c", "e"}},
		{3, "baz", []hashableString{"d", "a", "b"}},
		{2, "biz", []hashableString{"b", "a"}},
		{0, "boz", []hashableString{}},
		{100, "floo", []hashableString{"d", "a", "b", "c", "e"}},
	}

	for _, testcase := range testcases {
		gotNodes := hash.GetN(testcase.count, testcase.key)
		if !reflect.DeepEqual(gotNodes, testcase.expectedNodes) {
			t.Errorf("key=%q, count=%d - got: %v, expected: %v", testcase.key, testcase.count, gotNodes, testcase.expectedNodes)
		}
	}
}

func BenchmarkHashGetN3_5_nodes(b *testing.B) {
	hash := New(hashableString("a"), hashableString("b"), hashableString("c"), hashableString("d"), hashableString("e"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.GetN(3, sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkHashGetN5_5_nodes(b *testing.B) {
	hash := New(hashableString("a"), hashableString("b"), hashableString("c"), hashableString("d"), hashableString("e"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.GetN(5, sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkHashGetN3_10_nodes(b *testing.B) {
	hash := New(hashableString("a"), hashableString("b"), hashableString("c"), hashableString("d"), hashableString("e"), hashableString("f"), hashableString("g"), hashableString("h"), hashableString("i"), hashableString("j"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.GetN(3, sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkHashGetN5_10_nodes(b *testing.B) {
	hash := New(hashableString("a"), hashableString("b"), hashableString("c"), hashableString("d"), hashableString("e"), hashableString("f"), hashableString("g"), hashableString("h"), hashableString("i"), hashableString("j"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.GetN(5, sampleKeys[i%len(sampleKeys)])
	}
}

func TestHashRemove(t *testing.T) {
	nodes := []hashableString{"a", "b", "c"}
	hash := New(nodes...)

	var keyForB string
	nodeB := hashableString("b")

	for i := 0; i < 10000; i++ {
		randomKey := fmt.Sprintf("key-%d", i)
		if node, ok := hash.Get(randomKey); ok && node == nodeB {
			keyForB = randomKey
			break
		}
	}

	if keyForB == "" {
		t.Fatalf("Failed to find a key that maps to node %v", nodeB)
	}

	hash.Remove(nodeB)

	newNode, ok := hash.Get(keyForB)
	if !ok {
		t.Errorf("Key %q does not map to any node after removing %v", keyForB, nodeB)
	} else if newNode == nodeB {
		t.Errorf("Key %q still maps to removed node %v (%v)", keyForB, nodeB, newNode)
	}
}
