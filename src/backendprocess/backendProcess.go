package backendprocess

import (
	"encoding/json"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	md5simd "github.com/minio/md5-simd"
	"go.uber.org/atomic"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"tail-based-sampling/src/proto"
	"tail-based-sampling/src/util"
	"time"
)

type TraceIdBatch struct {
	TraceIdSet   mapset.Set
	BatchPos     int
	ProcessCount atomic.Int32
}

//TODO 该 slice 会不会有多线程问题
var TraceIdBatchSlice []TraceIdBatch
var RecNumPerBatch int32 //每个Batch需要收到的 setTraceIds 请求的数量
func init() {
	TraceIdBatchSlice = make([]TraceIdBatch, util.KBatchCount)
	RecNumPerBatch = int32(util.KProcessCount * util.KClientConcurrentNum)
}

var bigMu sync.Mutex

func SetWrongTraceId(c *gin.Context) {
	//log.Println("dasdasddasd")
	jsonStr := c.PostForm("traceIdListJson")
	batchPos, _ := strconv.Atoi(c.PostForm("batchPos"))

	//log.Println("jsonStr: ", jsonStr)
	//log.Println("batchPos: ", batchPos)

	var ss proto.TraceIds
	ss.UnmarshalJSON(util.Str2bytes(jsonStr))

	wrongeTraceSet := mapset.NewSet()
	for _, traceId := range ss.TraceIds {
		wrongeTraceSet.Add(traceId)
	}

	//json.Unmarshal([]byte(jsonStr), &wrongeTraceSet)

	pos := batchPos % util.KBatchCount
	if TraceIdBatchSlice[pos].BatchPos != 0 && TraceIdBatchSlice[pos].BatchPos != batchPos {
		log.Fatalln("ERROR overwrite traceId batch when call setWrongTraceId",
			"  TraceIdBatchSlice[pos].BatchPos: ", TraceIdBatchSlice[pos].BatchPos, "batchPos: ", batchPos)
	}
	//6f19e284fb2b4c6a
	bigMu.Lock()
	TraceIdBatchSlice[pos].BatchPos = batchPos
	TraceIdBatchSlice[pos].ProcessCount.Inc()
	if TraceIdBatchSlice[pos].TraceIdSet == nil {
		TraceIdBatchSlice[pos].TraceIdSet = wrongeTraceSet
	} else if wrongeTraceSet.Cardinality() > 0 {
		TraceIdBatchSlice[pos].TraceIdSet = TraceIdBatchSlice[pos].TraceIdSet.Union(wrongeTraceSet)
	}
	bigMu.Unlock()
	c.String(http.StatusOK, "suc")
}

var FinishProcessCount = atomic.NewInt32(0)

func Finish(c *gin.Context) {
	FinishProcessCount.Inc()
	c.String(http.StatusOK, "suc")
}

//保存最后计算的 md5 结果
var traceMd5Map = make(map[string]string)
var mutex sync.Mutex
var ssss atomic.Int32 //用于记录wrongTraceId的总数量

func Process() {
	ssss.Store(0)
	var ports []string
	ports = append(ports, util.KClientProcessPort1)
	ports = append(ports, util.KClientProcessPort2)

	server := md5simd.NewServer()
	defer server.Close()

	md5HashPool := &sync.Pool{
		New: func() interface{} {
			md5Hash := server.NewHash()
			return md5Hash
		},
	}

	spanSlicePool := &sync.Pool{
		New: func() interface{} {
			slice := make(util.SpanSlice, 0, 2048)
			return slice
		},
	}

	wg := sync.WaitGroup{}

	for {
		traceIdBatch, ok := getFinishedBatch()

		if !ok {
			if isFinished() {
				break
			}

			time.Sleep(10 * time.Millisecond)
			continue
		}

		//--------------------------------------------------------------
		wg.Add(1)

		go func(traceIdBatch TraceIdBatch) {

			md5Hash := md5HashPool.Get().(md5simd.Hasher)

			//log.Println("traceIdBatch: ", traceIdBatch)
			var batchPos int
			batchPos = traceIdBatch.BatchPos

			ch := make(chan util.TraceMap, 2)
			for _, clientPort := range ports {
				go func(clientPort string) {
					traceMap := getWrongTrace(traceIdBatch.TraceIdSet, batchPos, clientPort)
					ch <- traceMap
				}(clientPort)
			}

			ssss.Add(int32(traceIdBatch.TraceIdSet.Cardinality()))
			traceMap1 := <-ch
			traceMap2 := <-ch
			traceIdSet := mapset.NewSet()
			for id, _ := range traceMap1 {
				traceIdSet.Add(string(id))
			}
			for id, _ := range traceMap2 {
				traceIdSet.Add(string(id))
			}

			for id := range traceIdSet.Iter() {
				spanSlice := spanSlicePool.Get().(util.SpanSlice)
				spanSlice = append(spanSlice, traceMap1[id.(string)]...)
				spanSlice = append(spanSlice, traceMap2[id.(string)]...)
				sort.Sort(spanSlice)

				md5Hash.Reset()
				for _, span := range spanSlice {
					md5Hash.Write(span)
				}

				digest := md5Hash.Sum([]byte{})

				spanSlicePool.Put(spanSlice[:0])

				mutex.Lock()
				traceMd5Map[id.(string)] = fmt.Sprintf("%x", digest)
				mutex.Unlock()
			}
			md5HashPool.Put(md5Hash)
			wg.Done()
		}(traceIdBatch)
	}

	wg.Wait()

	sendMd5Result()

	for _, clientPort := range ports {
		exitClient(clientPort)
	}
	//log.Println("error trace num: ", ssss.Load())
	//log.Println("alltrace  num: ", allTraceId.Cardinality())
	os.Exit(0)
}

// 判断 wrongeTraceId 是否全部处理完毕
func isFinished() bool {
	if int(FinishProcessCount.Load()) < util.KProcessCount {
		return false
	}

	for i := 0; i < util.KBatchCount; i++ {
		if TraceIdBatchSlice[i].BatchPos != 0 {
			return false
		}
	}

	return true
}

var finishIndex int = 0

func getFinishedBatch() (TraceIdBatch, bool) {
	next := (finishIndex + 1) % util.KBatchCount

	bigMu.Lock()
	defer bigMu.Unlock()
	currentBatch := TraceIdBatchSlice[finishIndex]
	nextBatch := TraceIdBatchSlice[next]

	if (int(FinishProcessCount.Load()) >= util.KProcessCount && currentBatch.BatchPos > 0) ||
		(currentBatch.ProcessCount.Load() >= RecNumPerBatch && nextBatch.ProcessCount.Load() >= RecNumPerBatch) {

		TraceIdBatchSlice[finishIndex] = TraceIdBatch{}

		finishIndex = next
		return currentBatch, true
	}

	//log.Println("TraceIdBatchSlice: ", TraceIdBatchSlice)
	//log.Println("currentIndex: ", finishIndex)
	return TraceIdBatch{}, false
}

//关闭 clientprocess,这种方式不够优雅
func exitClient(port string) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/exit", port))
	if err == nil {
		defer resp.Body.Close()
	}
}

func sendMd5Result() bool {
	//fmt.Println(traceMd5Map)
	result, _ := json.Marshal(traceMd5Map)
	data := make(url.Values)
	data.Add("result", util.Bytes2str(result))
	resp, err := http.PostForm(fmt.Sprintf("http://localhost:%s/api/finished", util.KSamplingPort), data)
	if err == nil {
		defer resp.Body.Close()
	} else {
		log.Fatalln(err)
	}

	//log.Printf("%s\n", result)
	ioutil.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		//log.Println("suc to sendCheckSum, result:" + util.Bytes2str(result))
		return true
	}

	//log.Println("fail to sendCheckSum:" + resp.Status)

	return false
}

func getWrongTrace(traceIdSet mapset.Set, batchPos int, clientPort string) util.TraceMap {
	var traceIds proto.TraceIds
	traceIds.TraceIds = make([]string, 0, 512)
	for traceId := range traceIdSet.Iter() {
		traceIds.TraceIds = append(traceIds.TraceIds, traceId.(string))
	}
	//traceIdList, _ := json.Marshal(traceIdSet)
	traceIdList, _ := traceIds.MarshalJSON()
	data := make(url.Values)
	data.Add("traceIdList", util.Bytes2str(traceIdList))
	data.Add("batchPos", strconv.Itoa(batchPos))
	resp, err := http.PostForm(fmt.Sprintf("http://localhost:%s/getWrongTrace", clientPort), data)
	if err == nil {
		defer resp.Body.Close()
	} else {
		log.Fatalln(err)
	}

	body, _ := ioutil.ReadAll(resp.Body)
	//wrongTraceMap := make(util.TraceMap)
	//json.Unmarshal(body, &wrongTraceMap)
	var mm proto.TraceMap
	mm.UnmarshalJSON(body)
	return mm.Traces
}
