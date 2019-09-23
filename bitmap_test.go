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

	bm.Print()
	fmt.Println(bm.Remove(10))
	fmt.Println(bm.Remove(10))
	bm.Print()

}

func TestBitmap_Dump(t *testing.T) {

	bm := NewBitmap(1 << 10)
	fmt.Println(len(bm.bits), bm.maxSize)
	for i := 0; i < len(bm.bits); i++ {
		bm.bits[i] = 1094861636
	}
	f, err := os.OpenFile("./test.bf", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bm.Dump(f)
	f.Close()
	fmt.Println()
	bm.Print()
}

func TestLoadFile(t *testing.T) {
	f, err := os.OpenFile("./test.bf", os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	bm,err := LoadFile(f)
	fmt.Println(len(bm.bits),bm.maxSize)
	if err != nil {
		fmt.Println(err)
		return
	}
	bm.Print()
	bm.Dump(os.Stdout)
}
