package main

import (
	"os"
	"testing"

	"github.com/willf/bloom"
)

func TestExistFile(t *testing.T) {
	isExistFile := existFile("main.go")
	notExistFile := existFile("main1.go")

	if !isExistFile {
		t.Errorf("file %s should exist", "main.go")
	}

	if notExistFile {
		t.Errorf("file %s should not exist", "main1.go")
	}
}

func TestExistBloom(t *testing.T) {
	prefix := "prefix"
	bloomMap[prefix] = &bloom.BloomFilter{}
	exist := existBloom(prefix)
	if !exist {
		t.Errorf("bloom file with prefix: %s", prefix)
	}

	notExist := existBloom("not exist")
	if notExist {
		t.Errorf("bloom file with prefix: %s should not exist", prefix)
	}
}

// func TestGetBloomFilter(t *testing.T) {
// 	prefix := "test"
// 	filter := getBloomFilter(prefix)
// 	defer os.Remove(getBloomFileName(prefix))
// 	bloomMap = make(map[string]*bloom.BloomFilter)
// 	newFilter := getBloomFilter(prefix)

// 	if filter != newFilter {
// 		t.Fatalf("%p should be equal to %p", filter, newFilter)
// 	}
// }

func TestAddURL(t *testing.T) {
	prefix := "test"
	url := "http://www.example.com"
	filter := getBloomFilter(prefix)
	defer os.Remove(getBloomFileName(prefix))
	filter.Add([]byte(url))

	writeBloomFile(prefix, filter)

	exist, _ := existURL(url, prefix)
	if !exist {
		t.Fatalf("%s should exist with %s bloom", url, prefix)
	}
	bloomMap = make(map[string]*bloom.BloomFilter)
	loadBloom()

	exist, _ = existURL(url, prefix)
	if !exist {
		t.Fatalf("%s should exist with %s when bloom file exist", url, prefix)
	}
}
