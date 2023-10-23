package main

import (
	bitcask "bitcaskv"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
)

var db *bitcask.DB

func InitDB() {
	var err error
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func main() {
	InitDB()
	r := gin.Default()
	r.GET("/bitcask", getHandler)
	r.PUT("/bitcask", putHandler)
	r.DELETE("/bitcask", deleteHandler)
	r.GET("/bitcask/keys", listKeysHandler)
	r.GET("/bitcask/stat", statHandler)

	panic(r.Run(":8080"))
}

type GetForm struct {
	Key string `json:"keys" binding:"required"`
}

type DeleteForm struct {
	Keys []string `json:"keys" binding:"required"`
}

type PutForm struct {
	Kv map[string]string `json:"kv" binding:"required"`
}

type StatForm struct {
}

type ResponseData struct {
	Message interface{} `json:"msg"`
	Data    interface{} `json:"data,omitempty"`
}

func getHandler(c *gin.Context) {
	var form = GetForm{}
	if err := c.ShouldBindJSON(&form); err != nil {
		c.JSON(http.StatusOK, &ResponseData{Message: "invalid params"})
	}
	value, err := db.Get([]byte(form.Key))
	if err != nil {
		c.JSON(http.StatusInternalServerError, &ResponseData{Message: fmt.Sprintf("get value from db failed: %v", err)})
	}
	c.JSON(http.StatusOK, &ResponseData{Message: "success", Data: string(value)})

}

func putHandler(c *gin.Context) {
	var form PutForm
	if err := c.ShouldBindJSON(&form); err != nil {
		c.JSON(http.StatusOK, &ResponseData{Message: "invalid params"})
	}
	for key, value := range form.Kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			c.JSON(http.StatusOK, &ResponseData{Message: fmt.Sprintf("bitcask put data failed: %v", err)})
		}
	}
	c.JSON(http.StatusOK, &ResponseData{Message: "success"})

}

func deleteHandler(c *gin.Context) {
	var form DeleteForm
	if err := c.ShouldBindJSON(&form); err != nil {
		c.JSON(http.StatusOK, &ResponseData{Message: "invalid params"})
	}
	for _, key := range form.Keys {
		if err := db.Delete([]byte(key)); err != nil {
			c.JSON(http.StatusOK, &ResponseData{Message: fmt.Sprintf("bitcask delete data failed: %v", err)})
		}
	}
	c.JSON(http.StatusOK, &ResponseData{Message: "success"})
}

func listKeysHandler(c *gin.Context) {
	keys := db.ListKeys()
	if len(keys) == 0 {
		c.JSON(http.StatusOK, &ResponseData{Message: "empty list"})
	}
	data := make([]string, len(keys))
	for i, key := range keys {
		data[i] = string(key)
	}
	c.JSON(http.StatusOK, &ResponseData{Message: "success", Data: data})
}

func statHandler(c *gin.Context) {
	c.JSON(http.StatusOK, &ResponseData{Message: "success", Data: db.Stat()})
}
