package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zoroqi/bloomfilter"
	"os"
	"os/signal"
	"syscall"
)

type Result struct {
	Code   int         `json:"code"`
	Result interface{} `json:"result"`
}

func main() {

	bf := bloomfilter.NewBloomFilter(100, 0.001, "./", true)
	err := bf.Initialization()
	if err != nil {
		fmt.Println("init error", err)
		return
	}
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.Use(func(context *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				context.JSON(500, Result{Code: 500, Result: err})

			}
		}()
	})
	r.GET("put/:key", getPut(bf))
	r.POST("put", postPut(bf))
	r.GET("contains/:key", getContains(bf))
	//r.POST("contains/:key", contains(bf))
	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		<-signalChan
		bf.Close()
		syscall.Exit(0)
	}()
	err = r.Run(":9090")
	if err != nil {
		fmt.Println(err)
	}
}

func postPut(bf *bloomfilter.BF) gin.HandlerFunc {
	return func(context *gin.Context) {
		r := Result{Code: 200, Result: false}
		context.Request.ParseForm()
		key := context.Request.PostFormValue("key")
		exist := put(bf, key)
		r.Result = exist
		context.JSON(200, r)
	}
}

func getPut(bf *bloomfilter.BF) gin.HandlerFunc {
	return func(context *gin.Context) {
		r := Result{Code: 200, Result: false}
		key := context.Param("key")
		exist := put(bf, key)
		r.Result = exist
		context.JSON(200, r)
	}
}

func put(bf *bloomfilter.BF, str string) bool {
	return bf.Put(str)
}

func getContains(bf *bloomfilter.BF) gin.HandlerFunc {
	return func(context *gin.Context) {
		r := Result{Code: 200, Result: false}
		key := context.Param("key")
		if key == "" {
			context.JSON(200, r)
			return
		}
		exist := bf.Exist(key)
		r.Result = exist
		context.JSON(200, r)
	}
}
