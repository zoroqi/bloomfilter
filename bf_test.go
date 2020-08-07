package bloomfilter

import (
	"testing"
)

func TestBF_Put(t *testing.T) {
	bf := NewBloomFilter(500, 0.0005)
	bf.Put("123456")
	if !bf.Contains("123456") {
		t.Error("123456 contains")
	}
	if bf.Contains("1234567") {
		t.Error("1234567 not contains")
	}
}

func TestBF_Dump(t *testing.T) {
	bf := NewBloomFilter(40000000, 0.01)
	bf.Put("123456")
	bf.Contains("123456")
	err := bf.Dump("./", "test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestBF_Load(t *testing.T) {
	bf, err := LoadBF("./", "test")
	if err != nil {
		t.Fatal(err)
	}
	if !bf.Contains("123456") {
		t.Error("123456 contains")
	}
	if bf.Contains("1234567") {
		t.Error("1234567 not contains")
	}
}
