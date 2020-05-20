package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/willf/bloom"
)

var (
	port     = flag.String("port", "2020", "服务监听端口")
	host     = flag.String("host", "127.0.0.1", "服务监听地址")
	bloomMap = make(map[string]*bloom.BloomFilter)
	mutex    sync.Mutex
	bloomDir = "./bloom/"

	ErrBloomFileNil   = errors.New("布隆文件为空")
	ErrBloomFilterNil = errors.New("布隆对象为空")
	ErrOpenBloomFile  = errors.New("打开布隆文件失败")
	ErrWriteBloomFile = errors.New("写入布隆文件失败")
	ErrGetBloomFile   = errors.New("获取布隆文件失败")
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	loadBloom()         // 加载布隆文
	go receiveSignal()  // 监听停止信号，写入布隆文件
	go WriteBloomFile() // 定时写入布隆文件

	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
	log.Printf("server on http://127.0.0.1:%s\n", *port)
}

func loadBloom() {
	files, err := ioutil.ReadDir(bloomDir)
	if err != nil {
		log.Fatal(ErrGetBloomFile.Error())
	}

	for _, file := range files {
		fileName := file.Name()
		filePath := bloomDir + fileName
		f, err := os.Open(filePath)

		if err != nil {
			continue
		}

		filter := newBloomFilter()
		filter.ReadFrom(f)
		bloomMap[strings.Split(fileName, "_")[0]] = filter
		log.Printf("加载%s完毕\n", filePath)
	}
	log.Println("加载布隆文件完成")
}

func receiveSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	log.Println("接收到停止信号,开始写入布隆文件...")
	for k, v := range bloomMap {
		log.Printf("开始写入布隆文件:%s\n", getBloomFileName(k))
		writeBloomFile(k, v)
	}
	log.Println("写入布隆文件完成")
	os.Exit(1)
}

// WriteBloomFile 循环所有布隆写入文件
func WriteBloomFile() {
	for {

		time.Sleep(5 * time.Minute)

		for k, v := range bloomMap {
			log.Printf("写入布隆文件：%s\n", k)
			writeBloomFile(k, v)
		}

	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	prefix := getParam(r, "prefix")
	url := getParam(r, "url")

	if len(prefix) == 0 {
		fmt.Fprintf(w, "prefix参数错误")
		return
	}

	exist, err := existURL(url, prefix)
	if err != nil {
		fmt.Fprintf(w, "%v", err.Error())
	} else {
		fmt.Fprintf(w, "%v", exist)
	}

}

// getParam 获取url参数
func getParam(r *http.Request, key string) string {
	values, ok := r.URL.Query()[key]
	if !ok || len(values) == 0 {
		return ""
	}
	return values[0]
}

// existFile 文件是否存在
func existFile(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

// existBloom 布隆文件是否存在
func existBloom(prefix string) bool {
	_, ok := bloomMap[prefix]
	if !ok {
		if existFile(getBloomFileName(prefix)) {
			// 如果存在本地文件
			getBloomFilter(prefix)
			return true
		}
	}
	return ok
}

// getBloomFileName 获取布隆文件地址
func getBloomFileName(prefix string) string {
	return bloomDir + prefix + "_bloomfilter.txt"
}

// existURL url是否在布隆中
func existURL(url, prefix string) (bool, error) {
	bloomFilter := getBloomFilter(prefix)
	if bloomFilter == nil {
		return false, ErrBloomFilterNil
	}
	exist := bloomFilter.Test([]byte(url))
	addURL(url, prefix)
	return exist, nil
}

// addURL 将url添加到布隆里面
func addURL(url, prefix string) {
	mutex.Lock()
	bloomFilter := getBloomFilter(prefix)

	if bloomFilter == nil {
		log.Fatalf("%s url: %s, prefix: %s\n", ErrBloomFilterNil, url, prefix)
		return
	}

	bloomFilter.Add([]byte(url))
	mutex.Unlock()
}

func writeBloomFile(prefix string, bloomFilter *bloom.BloomFilter) {
	buffer := new(bytes.Buffer)
	bloomFilter.WriteTo(buffer)

	bloomFile := getBloomFileName(prefix)
	file, err := os.OpenFile(bloomFile, os.O_WRONLY, os.ModeAppend)
	defer file.Close()
	if err != nil {
		log.Fatalf("%s: %s", ErrOpenBloomFile.Error(), bloomFile)
		return
	}

	_, err = file.WriteString(buffer.String())
	if err != nil {
		log.Fatalf("%s:%s\n", ErrWriteBloomFile.Error(), err)
	}
}

func getBloomFilter(prefix string) *bloom.BloomFilter {
	bloomFilter, ok := bloomMap[prefix]
	if !ok {
		// 创建文件
		os.Create(getBloomFileName(prefix))
		bloomMap[prefix] = newBloomFilter()
		return bloomMap[prefix]
	}
	return bloomFilter
}

func newBloomFilter() *bloom.BloomFilter {
	return bloom.New(2949754730, 20)
}

// existURLInBloom 布隆判定url是否存在
func existURLInBloom(url, prefix string) bool {
	bloomFilter := getBloomFilter(prefix)
	if bloomFilter == nil {
		log.Fatalf(ErrBloomFilterNil.Error())
		return false
	}

	return bloomFilter.Test([]byte(url))
}
