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
	"strconv"
	"sync"
	"tail-based-sampling/src/util"
	"time"
)

type TraceIdBatch struct {
	TraceIdSet   mapset.Set
	BatchPos     int
	ProcessCount int
}

//TODO 该 slice 会不会有多线程问题
var TraceIdBatchSlice []TraceIdBatch
var FinishBatchChan chan int

func init() {
	TraceIdBatchSlice = make([]TraceIdBatch, util.KBatchCount)
	FinishBatchChan = make(chan int, util.KBatchCount)
}

func SetWrongTraceId(c *gin.Context) {
	//log.Println("dasdasddasd")
	jsonStr := c.PostForm("traceIdListJson")
	batchPos, _ := strconv.Atoi(c.PostForm("batchPos"))

	//log.Println("jsonStr: ", jsonStr)
	//log.Println("batchPos: ", batchPos)

	wrongeTraceSet := mapset.NewSet()
	json.Unmarshal([]byte(jsonStr), &wrongeTraceSet)

	pos := batchPos % util.KBatchCount
	if TraceIdBatchSlice[pos].BatchPos != 0 && TraceIdBatchSlice[pos].BatchPos != batchPos {
		log.Fatalln("ERROR overwrite traceId batch when call setWrongTraceId",
			"  TraceIdBatchSlice[pos].BatchPos: ", TraceIdBatchSlice[pos].BatchPos, "batchPos: ", batchPos)
	}

	TraceIdBatchSlice[pos].BatchPos = batchPos
	TraceIdBatchSlice[pos].ProcessCount++
	if TraceIdBatchSlice[pos].TraceIdSet == nil {
		TraceIdBatchSlice[pos].TraceIdSet = wrongeTraceSet
	} else if wrongeTraceSet.Cardinality() > 0 {
		TraceIdBatchSlice[pos].TraceIdSet = TraceIdBatchSlice[pos].TraceIdSet.Union(wrongeTraceSet)
	}
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

func Process() {

	var ports []string
	ports = append(ports, util.KClientProcessPort1)
	ports = append(ports, util.KClientProcessPort2)

	server := md5simd.NewServer()
	defer server.Close()

	wg := sync.WaitGroup{}

	for {
		traceIdBatch, ok := getFinishedBatch()

		//log.Println("TraceIdBatchSlice: ", TraceIdBatchSlice, " ok: ", ok)
		if !ok {
			if isFinished() {
				break
			}

			time.Sleep(1 * time.Millisecond)
			continue
		}

		//--------------------------------------------------------------
		wg.Add(1)

		go func(traceIdBatch TraceIdBatch) {

			md5Hash := server.NewHash()
			defer md5Hash.Close()

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
				i, j := 0, 0
				ilen, jlen := -1, -1
				spanSlice1, ok := traceMap1[util.TraceId(id.(string))]
				if ok {
					ilen = len(spanSlice1)
				}

				spanSlice2, ok := traceMap2[util.TraceId(id.(string))]
				if ok {
					jlen = len(spanSlice2)
				}

				md5Hash.Reset()

				flag1, flag2 := i < ilen, j < jlen
				for flag1 || flag2 {
					switch {
					case flag1 && flag2:
						if util.Bytes2str(spanSlice1[i]) < util.Bytes2str(spanSlice2[j]) {
							md5Hash.Write(spanSlice1[i])
							i++
						} else {
							md5Hash.Write(spanSlice2[j])
							j++
						}
					case flag1 && !flag2:
						md5Hash.Write(spanSlice1[i])
						i++
					case !flag1 && flag2:
						md5Hash.Write(spanSlice2[j])
						j++
					}
					flag1, flag2 = i < ilen, j < jlen
				}

				digest := md5Hash.Sum([]byte{})

				mutex.Lock()
				traceMd5Map[id.(string)] = fmt.Sprintf("%x", digest)
				mutex.Unlock()
			}

			wg.Done()

		}(traceIdBatch)
	}

	wg.Wait()

	sendMd5Result()

	for _, clientPort := range ports {
		exitClient(clientPort)
	}
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

	currentBatch := TraceIdBatchSlice[finishIndex]
	nextBatch := TraceIdBatchSlice[next]

	if (int(FinishProcessCount.Load()) >= util.KProcessCount && currentBatch.BatchPos > 0) ||
		(currentBatch.ProcessCount >= util.KProcessCount && nextBatch.ProcessCount >= util.KProcessCount) {
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

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		//log.Println("suc to sendCheckSum, result:" + util.Bytes2str(result))
		return true
	}

	//log.Println("fail to sendCheckSum:" + resp.Status)

	return false
}

func getWrongTrace(traceIdSet mapset.Set, batchPos int, clientPort string) util.TraceMap {
	traceIdList, _ := json.Marshal(traceIdSet)
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
	wrongTraceMap := make(util.TraceMap)
	json.Unmarshal(body, &wrongTraceMap)
	return wrongTraceMap
}
