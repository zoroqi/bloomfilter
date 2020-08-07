package bloomfilter

import (
	"fmt"
	gh "github.com/dgryski/go-farm"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

const FILE_BF_BITEMAP = ".bitmap"
const FILE_BF_META = ".meta"

type BF struct {
	bm       *Bitmap
	BitSize  uint64
	HashSize int
	MaxSize  uint64
	Total    uint64
}

func NewBloomFilter(max uint32, fpp float64) *BF {
	return &BF{
		bm:       NewBitmap(bitSize(max, fpp)),
		HashSize: hashNum(max, fpp),
		BitSize:  bitSize(max, fpp),
		MaxSize:  uint64(max),
		Total:    0,
	}
}

func hash(str string) uint64 {
	return gh.Hash64([]byte(str))
}

func bitSize(max uint32, fpp float64) uint64 {
	return uint64(-(float64(max)*math.Log(fpp))/(math.Log(2)*math.Log(2))) + 1
}

func hashNum(max uint32, fpp float64) int {
	return int(float64(bitSize(max, fpp)/uint64(max)) * math.Log(2))
}

func (b *BF) Put(str string) bool {
	r := true
	h1 := hash(str)
	h2 := hash(str + str)
	for i := 0; i < b.HashSize; i++ {
		c := h1 + uint64(i)*h2
		r = b.bm.Set(c%b.bm.maxSize) && r
	}

	if !r {
		atomic.AddUint64(&b.Total, 1)
	}
	return r
}

func (b *BF) Contains(str string) bool {
	r := true
	h1 := hash(str)
	h2 := hash(str + str)
	for i := 0; i < b.HashSize; i++ {
		c := h1 + uint64(i)*h2
		r = r && b.bm.Get(c%b.bm.maxSize)
		if !r {
			return r
		}
	}
	return r
}

func LoadBF(path string, name string) (*BF, error) {
	metaPath := path + "/" + name + FILE_BF_META
	meta, err := ioutil.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}
	bf, err := parseMeta(string(meta))
	if err != nil {
		return nil, err
	}

	bitPath := path + "/" + name + FILE_BF_BITEMAP
	bitFile, err := os.OpenFile(bitPath, os.O_RDONLY|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}
	defer bitFile.Close()
	bitFileInfo, err := bitFile.Stat()
	if err != nil {
		return nil, err
	}
	bm, err := LoadBitMap(bitFile, bitFileInfo.Size())
	if err != nil {
		return nil, err
	}
	bf.bm = bm
	return bf, nil
}

func parseMeta(meta string) (*BF, error) {
	ss := strings.Split(meta, ":")
	if len(ss) != 4 {
		return nil, fmt.Errorf("meta loss field")
	}
	bf := &BF{}
	var err error
	bf.MaxSize, err = strconv.ParseUint(ss[0], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("MaxSize %w", err)
	}
	bf.HashSize, err = strconv.Atoi(ss[1])
	if err != nil {
		return nil, fmt.Errorf("HashSize %w", err)
	}
	bf.BitSize, err = strconv.ParseUint(ss[2], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("BitSize %w", err)
	}
	bf.Total, err = strconv.ParseUint(ss[3], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("Total %w", err)
	}
	return bf, nil
}

func (b *BF) meta() string {
	return fmt.Sprintf("%d:%d:%d:%d", b.MaxSize, b.HashSize, b.BitSize, b.Total)
}

func (b *BF) Dump(path string, name string) error {
	bitPath := path + "/" + name + FILE_BF_BITEMAP
	metaPath := path + "/" + name + FILE_BF_META
	metaPathTemp := metaPath + ".temp"
	bitPathTemp := bitPath + ".temp"

	bitFile, err := os.OpenFile(bitPathTemp, os.O_RDWR|os.O_CREATE|os.O_SYNC|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer bitFile.Close()
	metaFile, err := os.OpenFile(metaPathTemp, os.O_RDWR|os.O_CREATE|os.O_SYNC|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer metaFile.Close()
	b.bm.Dump(bitFile)
	metaFile.Write([]byte(b.meta()))

	if err = os.Rename(metaPathTemp, metaPath); err != nil {
		return err
	}
	if err = os.Rename(bitPathTemp, bitPath); err != nil {
		return err
	}
	return nil
}
