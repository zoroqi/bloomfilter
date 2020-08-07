package bloomfilter

import (
	"fmt"
	"os"
	"testing"
)

func TestBitmap(t *testing.T) {
	bm := NewBitmap(100)
	fmt.Println(bm.Set(10))
	fmt.Println(bm.Set(10))
	fmt.Println(bm.Set(12))

	bm.debugPrint()
	fmt.Println(bm.Remove(10))
	fmt.Println(bm.Remove(10))
	bm.debugPrint()

}

func TestBitmap_Dump(t *testing.T) {
	bm := NewBitmap(1<<11 + 1)
	fmt.Println(len(bm.bits), bm.maxSize)
	for i := 0; i < len(bm.bits); i++ {
		bm.bits[i] = 1493544343
	}
	f, err := os.OpenFile("./test.bf", os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_SYNC, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bm.Dump(f)
	fmt.Println(len(bm.bits), bm.maxSize)
	f.Close()
}

func TestLoadFile(t *testing.T) {
	f, err := os.OpenFile("./test.bf", os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bm, err := LoadBitMap(f, 0)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(len(bm.bits), bm.maxSize)
}
