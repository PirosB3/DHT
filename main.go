package main

import (
	"container/list"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
)

const IdLength = 20

type NodeID [IdLength]byte

func NewNodeID(data string) NodeID {
	decoded, _ := hex.DecodeString(data)
	var result NodeID
	for i := 0; i < IdLength; i++ {
		result[i] = decoded[i]
	}
	return result
}

func NewRandomNodeID() NodeID {
	var res NodeID
	for i := 0; i < IdLength; i++ {
		res[i] = uint8(rand.Intn(256))
	}
	return res
}

func (node NodeID) String() string {
	return hex.EncodeToString(node[0:IdLength])
}

func (node NodeID) Equals(other NodeID) bool {
	for i := 0; i < IdLength; i++ {
		if node[i] != other[i] {
			return false
		}
	}
	return true
}

func (node NodeID) Less(other NodeID) bool {
	for i := 0; i < IdLength; i++ {
		if node[i] != other[i] {
			return node[i] < other[i]
		}
	}
	return false
}

func (node NodeID) Xor(other NodeID) NodeID {
	var res NodeID
	for i := 0; i < IdLength; i++ {
		res[i] = node[i] ^ other[i]
	}
	return res
}

func (node NodeID) PrefixLen() int {
	for i := 0; i < IdLength; i++ {
		for j := 0; j < 8; j++ {
			if node[i]&(1<<uint(7-j)) != 0 {
				return i*8 + j
			}
		}
	}
	return IdLength*8 - 1
}

const BucketSize = 20

type Contact struct {
	id NodeID
}

type RoutingTable struct {
	node    NodeID
	buckets [IdLength * 8]*list.List
}

func NewRoutingTable(node NodeID) RoutingTable {
	var res RoutingTable
	for i := 0; i < IdLength*8; i++ {
		res.buckets[i] = list.New()
	}
	res.node = node
	return res
}

func FindNodeInList(ptr *list.List, prefix interface{}) *list.Element {
	current := ptr.Front()
	for current != nil {
		if current.Value == prefix {
			return current
		}
	}
	return nil
}

func (table *RoutingTable) Update(contact *Contact) {
	xor := table.node.Xor(contact.id)
	prefix := xor.PrefixLen()

	selected_bucket := table.buckets[prefix]
	node_in_list := FindNodeInList(selected_bucket, contact)
	if node_in_list != nil {
		selected_bucket.MoveToFront(node_in_list)
	} else {
		if selected_bucket.Len() < BucketSize {
			selected_bucket.PushFront(contact)
		}
	}
}

type ContactRecord struct {
	node    *Contact
	sortKey NodeID
}

func (rec *ContactRecord) Less(other interface{}) bool {
	return rec.sortKey.Less(other.(*ContactRecord).sortKey)
}

type ContactRecordSlice []*ContactRecord

func (a ContactRecordSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ContactRecordSlice) Less(i, j int) bool { return a[i].Less(a[j]) }
func (cr ContactRecordSlice) Len() int          { return len(cr) }

func CopyXorToVector(start *list.List, vec []*ContactRecord, target NodeID) {
	current := start.Front()
	for current != nil {
		vec = append(vec, &ContactRecord{
			node:    current.Value.(*Contact),
			sortKey: target.Xor(current.Value.(*Contact).id),
		})
	}
}

func (table *RoutingTable) FindClosest(target NodeID, count int) []*ContactRecord {
	result := make(ContactRecordSlice, count)
	selected_bucket_idx := table.node.Xor(target).PrefixLen()
	CopyXorToVector(table.buckets[selected_bucket_idx], result, target)
	to_move := 1
	can_terminate_left := false
	can_terminate_right := false
	for can_terminate_left != true || can_terminate_right != true {
		if len(result) >= count {
			break
		}
		left := selected_bucket_idx - to_move
		right := selected_bucket_idx + to_move
		if left >= 0 {
			CopyXorToVector(table.buckets[left], result, target)
		} else {
			can_terminate_left = true
		}
		if right < IdLength*8 {
			CopyXorToVector(table.buckets[right], result, target)
		} else {
			can_terminate_right = true
		}
	}
	sort.Sort(result)
	return result
}

func main() {
	node := NewRandomNodeID()
	node2 := NewRandomNodeID()
	fmt.Println(node.Xor(node2).PrefixLen())
}
