package backendprocess

import (
	"encoding/json"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	md5simd "github.com/minio/md5-simd"
	"go.uber.org/atomic"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
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

func SetWrongTraceId(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	jsonStr := r.PostForm.Get("traceIdListJson")
	batchPos, _ := strconv.Atoi(r.PostForm.Get("batchPos"))

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
	w.Write([]byte("suc"))
}

var FinishProcessCount = atomic.NewInt32(0)

func Finish(w http.ResponseWriter, r *http.Request) {
	FinishProcessCount.Inc()
	w.Write([]byte("suc"))
}

//保存最后计算的 md5 结果
var traceMd5Map = make(map[string]string)

func Process() {
	var ports []string
	ports = append(ports, util.KClientProcessPort1)
	ports = append(ports, util.KClientProcessPort2)

	server := md5simd.NewServer()
	defer server.Close()

	md5Hash := server.NewHash()
	defer md5Hash.Close()

	for {
		traceIdBatch, ok := getFinishedBatch()

		//log.Println("TraceIdBatchSlice: ", TraceIdBatchSlice, " ok: ", ok)
		if !ok {
			if isFinished() {
				sendMd5Result()
				break
			}

			time.Sleep(1000 * time.Millisecond)
			continue
		}

		//临时保存 wrongTrace 的所有span
		var wrongTraceMap = make(util.TraceMap)

		//log.Println("traceIdBatch: ", traceIdBatch)
		var batchPos int
		if traceIdBatch.TraceIdSet.Cardinality() > 0 {
			batchPos = traceIdBatch.BatchPos

			for _, clientPort := range ports {
				traceMap := getWrongTrace(traceIdBatch.TraceIdSet, batchPos, clientPort)
				if traceMap != nil {
					for traceId := range traceMap {
						wrongTraceMap[traceId] = append(wrongTraceMap[traceId], traceMap[traceId]...)
					}
				}
			}
		}

		//计算 md5
		for traceId := range wrongTraceMap {
			//TODO 排序优化
			sort.Sort(wrongTraceMap[traceId])

			//TODO 异步化 md5 操作
			md5Hash.Reset()
			for span := range wrongTraceMap[traceId] {
				md5Hash.Write(wrongTraceMap[traceId][span])
			}
			digest := md5Hash.Sum([]byte{})
			traceMd5Map[string(traceId)] = fmt.Sprintf("%x", digest)
		}
		//log.Println("traceMd5Map.len: ", len(traceMd5Map))
	}

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
	result, _ := json.Marshal(traceMd5Map)
	data := make(url.Values)
	data.Add("result", util.Bytes2str(result))
	resp, err := http.PostForm(fmt.Sprintf("http://localhost:%s/api/finished", util.KSamplingPort), data)
	if err == nil {
		defer resp.Body.Close()
	} else {
		log.Fatalln(err)
	}

	log.Printf("%s\n", result)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		//log.Println("suc to sendCheckSum, result:" + util.Bytes2str(result))
		return true
	}

	log.Println("fail to sendCheckSum:" + resp.Status)

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
