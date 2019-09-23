package bloomfilter

import (
	gh "github.com/dgryski/go-farm"
	"github.com/sirupsen/logrus"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

const FILE_LOG = ".log"
const FILE_BF = ".bf"

type BF struct {
	bm       *Bitmap
	BitSize  uint64
	HashSize int
	MaxSize  uint64
	Total    int32
	Hash     []int
	append   *appendlog
}

type appendlog struct {
	log       *logrus.Logger
	buffer    chan string
	dump      bool
	localPath string
	logName   string
	stop      bool
	stopCh    chan struct{}
	wait      sync.WaitGroup
}

func (b *BF) Initialization() (e error) {
	return b.append.initialization()
}

func (b *BF) Close() {
	b.append.close()
}

func NewBloomFilter(max uint32, fpp float64, path string, isDump bool) *BF {
	return &BF{
		bm:       NewBitmap(bitSize(max, fpp)),
		HashSize: hashNum(max, fpp),
		BitSize:  bitSize(max, fpp),
		MaxSize:  uint64(max),
		Total:    0,
		append:   &appendlog{buffer: make(chan string, 1024), dump: isDump, localPath: path, logName: "bloomfiler", wait: sync.WaitGroup{}},
	}
}

func hash(str string) uint64 {
	return gh.Hash64([]byte(str))
}

func bitSize(max uint32, fpp float64) uint64 {
	return uint64(-(float64(max) * math.Log(fpp))/(math.Log(2)*math.Log(2))) + 1
}

func hashNum(max uint32, fpp float64) int {
	return int(float64(bitSize(max, fpp)/uint64(max)) * math.Log(2))
}

func (b *BF) Put(str string) bool {
	r := b.put(str)
	if !r {
		b.append.appendBuffer(str)
	}
	return r
}

func (b *BF) put(str string) bool {
	r := true
	h1 := hash(str)
	h2 := hash(str + str)
	for i := 0; i < b.HashSize; i++ {
		c := uint64(h1 + uint64(i)*h2)
		r = b.bm.Set(c%b.bm.maxSize) && r
	}

	if !r {
		atomic.AddInt32(&b.Total, 1)
	}
	return r
}

func (b *BF) Exist(str string) bool {
	r := true
	h1 := hash(str)
	h2 := hash(str + str)
	for i := 0; i < b.HashSize; i++ {
		c := uint64(h1 + uint64(i)*h2)
		r = r && b.bm.Exist(c%b.bm.maxSize)
		if !r {
			return r
		}
	}
	return r
}

func (b *appendlog) initialization() (e error) {
	b.wait.Add(1)
	b.stopCh = make(chan struct{})
	f, err := os.OpenFile(b.localPath+b.logName+FILE_LOG, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		e = err
		return
	}
	b.log = logrus.New()
	b.log.SetOutput(f)
	b.log.SetNoLock()
	b.log.SetFormatter(&logrus.TextFormatter{})

	b.asyncWriteLog()

	// 停止监听, 关闭文件和等待buffer消费完毕
	go func() {
		defer b.wait.Done()
		defer f.Close()
		_ = <-b.stopCh
		for s := range b.buffer {
			if s != "" {
				b.log.Infof("%s", s)
			}
		}
	}()
	return
}

func (b *appendlog) asyncWriteLog() {
	writeOnce := sync.Once{}
	writeOnce.Do(func() {
		go func(ch chan string, logger *logrus.Logger) {
			for {
				select {
				case s, open := <-ch:
					if open {
						if s != "" {
							logger.Infof("%s", s)
						}
					} else {
						break
					}
				}
			}
		}(b.buffer, b.log)
	})
}

func (b *appendlog) close() {
	b.stop = true
	close(b.buffer)
	close(b.stopCh)
	b.wait.Wait()
}

func (b *appendlog) appendBuffer(str string) {
	if b.dump && !b.stop {
		b.buffer <- str
	}
}
