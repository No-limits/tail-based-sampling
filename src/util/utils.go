package util

import (
	"net/http"
	"strconv"
	"strings"
	"unsafe"
)

var client = &http.Client{}

func CallHTTP(req *http.Request) (resp *http.Response, err error) {
	resp, err = client.Do(req)
	return
}

func IsClientProcess() bool {
	if KListenPort == KClientProcessPort1 || KListenPort == KClientProcessPort2 {
		return true
	}
	return false
}

func IsBackendProcess() bool {
	if KListenPort == KBackendProcessPort {
		return true
	}
	return false
}

//string convert to bytes, please pay attention! it doesn't use copy
func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

//bytes convert to string,  please pay attention! it doesn't use copy
func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

type Span []byte
type SpanSlice []Span
type TraceId string
type TraceMap map[TraceId]SpanSlice
type TraceMapSlice []struct {
	TraceMap TraceMap
	Count    int
} // BatchTraceList

func (s SpanSlice) Len() int {
	return len(s)
}

func (s SpanSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//根据 span 中 timestamp 排序，用于
func (s SpanSlice) Less(i, j int) bool {
	return getStartTime(s[i]) < getStartTime(s[j])
}

func getStartTime(span []byte) int64 {
	var low, high int
	low = strings.Index(Bytes2str(span), "|") + 1
	if low == 0 {
		return -1
	}

	high = strings.Index(Bytes2str(span[low:]), "|")
	if high == -1 {
		return -1
	}
	ret, _ := strconv.ParseInt(
		Bytes2str(span[low:low+high]), 10, 64)
	return ret
}
