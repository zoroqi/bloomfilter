package bloomfilter

import (
	"fmt"
	"testing"
)

func TestBF_Put(t *testing.T) {
	bf := NewBloomFilter(500, 0.0005)
	fmt.Println(bf.HashSize, bf.Total, bf.MaxSize,bf.BitSize/32,bf.bm.maxSize)
	bf.Put("123456")
	bf.bm.Print()
	fmt.Println(bf.HashSize, bf.Total, bf.MaxSize,bf.BitSize/32)
}
