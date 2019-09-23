package bloomfilter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
)

type Bitmap struct {
	bits    []uint32
	maxSize uint64
}

const (
	unit = 32
)

// 静态添加使用
var as []uint32
// 静态移除使用
var rs []uint32

func init() {
	as = make([]uint32, unit)
	rs = make([]uint32, unit)
	for i := uint(0); i < unit; i++ {
		as[i] = uint32(1 << i)
	}
	r := uint32(1<<unit - 1)
	for i := uint(0); i < unit; i++ {
		rs[i] = uint32(r ^ 1<<i)
	}
}

func NewBitmap(size uint64) *Bitmap {
	s := uint64(size / unit)
	if size%unit != 0 {
		s = s + 1
	}
	return &Bitmap{bits: make([]uint32, s), maxSize: s * unit}
}

func LoadFile(read io.Reader) (*Bitmap, error) {
	bits, err := load(read)
	if err != nil {
		return nil, err
	}
	return &Bitmap{bits: bits, maxSize: uint64(len(bits) * unit)}, nil
}

func load(read io.Reader) ([]uint32, error) {
	r := make([]uint32, 0, 1024)
	bs := make([]byte, 1024)
	for {
		n, err := read.Read(bs)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}
		num := n/4
		for i := 0; i < num; i++ {
			r = append(r, binary.BigEndian.Uint32(bs[i:i+4]))
		}

	}
	return r, nil
}

func (b *Bitmap) verifyLength(site uint64) {
	if site > b.maxSize {
		panic(fmt.Sprintf("max length %d", b.maxSize))
	}
}

// return true:before value 1, false before 0
func (b *Bitmap) Set(index uint64) bool {
	b.verifyLength(index)
	bucket := index / unit
	s := index % unit
	r := atomic.LoadUint32(&b.bits[bucket])
	if atomic.CompareAndSwapUint32(&b.bits[bucket], r, r|as[s]) {
		return r&as[s] == as[s]
	}

	for {
		r = atomic.LoadUint32(&b.bits[bucket])
		if atomic.CompareAndSwapUint32(&b.bits[bucket], r, r|as[s]) {
			return r&as[s] == as[s]
		}
	}
}

// return true:value 1, before 0
func (b *Bitmap) Exist(index uint64) bool {
	b.verifyLength(index)
	return atomic.LoadUint32(&b.bits[index/unit])&as[index%unit] == as[index%unit]
}

// return true:before value 1, false before 0
func (b *Bitmap) Remove(index uint64) bool {
	b.verifyLength(index)
	bucket := index / unit
	s := index % unit
	r := atomic.LoadUint32(&b.bits[bucket])
	if atomic.CompareAndSwapUint32(&b.bits[bucket], r, r&rs[s]) {
		return r&as[s] == as[s]
	}
	for {
		r = atomic.LoadUint32(&b.bits[bucket])
		if atomic.CompareAndSwapUint32(&b.bits[bucket], r, r&rs[s]) {
			return r&as[s] == as[s]
		}
	}
}

func (b *Bitmap) Dump(w io.Writer) error {
	return dump(w, b.bits)
}

func dump(w io.Writer, bits []uint32) error {
	bs := make([]byte, 1024)
	length := len(bits)
	i := 0
	for {
		if i >= length {
			break
		}
		if i+256 < length {
			fill(bs, bits[i:i+256])
		} else {
			fill(bs, bits[i:length])
			bs = bs[0 : (length-i)*4]
		}
		i = i + 256
		w.Write(bs)
	}
	return nil
}

func (b *Bitmap) Print() {
	s := "%4d ~%4d:%0" + strconv.Itoa(unit) + "b\n"
	for i, v := range b.bits {
		n := i % unit
		if n == 0 {
			fmt.Printf("----- %d -----\n", i*unit)
		}
		fmt.Printf(s, (n+1)*unit-1, n*unit, v)
	}
}

func fill(bs []byte, arr []uint32) error {
	if (len(bs)+1)/4 < len(arr) {
		return errors.New(fmt.Sprintf("[] byte len < %d", len(arr)*4))
	}
	for i, v := range arr {
		binary.BigEndian.PutUint32(bs[i*4:(i+1)*4], v)
	}
	return nil
}
