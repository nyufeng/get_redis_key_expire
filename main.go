package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalln(".env 文件初始化错误", err)
	}

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisPass := os.Getenv("REDIS_PASS")
	redisDB, _ := strconv.Atoi(os.Getenv("REDIS_DB"))

	dateTime := time.Now().Format("20060102150405")
	w, err := os.OpenFile("out_redis_key_expire_list_"+dateTime+".csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalln("输出文件打开错误", err)
	}
	defer w.Close()

	keyExpireOutCSV := csv.NewWriter(w)
	defer keyExpireOutCSV.Flush()

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: redisPass,
		DB:       redisDB,
	})

	pingResult := rdb.Ping(ctx)
	if pingResult.Err() != nil {
		log.Fatalln("redis 连接错误", pingResult.Err())
	}
	log.Println("redis 连接成功")

	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = rdb.Scan(ctx, cursor, "*", 1000).Result()
		if err != nil {
			log.Fatalln("redis 读取错误", err)
		}

		for _, key := range keys {
			expireResult := rdb.TTL(ctx, key).Val()
			typeResult := rdb.Type(ctx, key).Val()
			keyExpireOutCSV.Write([]string{key, expireResult.String(), typeResult})
		}

		if cursor == 0 {
			break
		}
	}
	log.Println("生成成功")
}
