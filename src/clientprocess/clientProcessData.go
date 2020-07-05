package clientprocess

import (
	"bytes"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"tail-based-sampling/src/proto"
	"tail-based-sampling/src/util"
	"time"
)

var BatchTraceList util.BatchTraceLists
var LineChans []chan []byte      //并行处理行的channel
var BatchSizePerGo int           //每个batch由多个协程并行处理后，各自负责的数据量大小
var ChanInitSize int             //初始化管道的大小
var LineProcessWg sync.WaitGroup //等待并行处理的协程全部退出

func init() {
	BatchTraceList = make(util.BatchTraceLists, util.KBatchCount)
	BatchSizePerGo = util.KBatchSize / util.KClientConcurrentNum
	ChanInitSize = 200000

	LineChans = make([]chan []byte, util.KClientConcurrentNum)
	LineProcessWg.Add(util.KClientConcurrentNum)
	for i := 0; i < util.KClientConcurrentNum; i++ {
		LineChans[i] = make(chan []byte, ChanInitSize)
		go dealLine(i)
	}

	for i := 0; i < len(BatchTraceList); i++ {
		BatchTraceList[i].TraceMapSlice = make([]util.TraceMap, util.KClientConcurrentNum)
	}
	BatchTraceList[0].Count = 1
}

func GetWrongTrace(c *gin.Context) {
	wrongTraceSetStr := c.PostForm("traceIdList")
	batchPos, _ := strconv.Atoi(c.PostForm("batchPos"))

	//log.Println("wrongTraceSet: ", wrongTraceSetStr)
	//log.Println("get wrongtrace batchPos: ", batchPos)

	data := getWrongTracing(wrongTraceSetStr, batchPos)

	c.Writer.Write(data)
}

// RestFul 接口实际调用的本函数，根据 TraceIds 获得所有的日志
func getWrongTracing(wrongTraceSetStr string, batchPos int) []byte {

	//wrongTraceSet := mapset.NewSet()
	//json.Unmarshal([]byte(wrongTraceSetStr), &wrongTraceSet)

	var traceIds proto.TraceIds
	traceIds.UnmarshalJSON(util.Str2bytes(wrongTraceSetStr))

	pos := batchPos % util.KBatchCount
	pre := pos - 1
	if pre == -1 {
		pre = util.KBatchCount - 1
	}
	next := pos + 1
	if next == util.KBatchCount {
		next = 0
	}

	traceMap := make(util.TraceMap)
	getWrongTracingWithBatch := func(pos int) {
		for _, traceId := range traceIds.TraceIds {
			for i := 0; i < util.KClientConcurrentNum; i++ {
				if BatchTraceList[pos].TraceMapSlice[i] != nil {
					traceMap[traceId] = append(traceMap[traceId], BatchTraceList[pos].TraceMapSlice[i][traceId]...)
				}
			}
		}
	}

	getWrongTracingWithBatch(pre)
	getWrongTracingWithBatch(pos)
	getWrongTracingWithBatch(next)

	//if batchPos != 0 {
	if BatchTraceList[pre].TraceMapSlice[0] != nil {
		BatchTraceList[pre].Count++
		if BatchTraceList[pre].Count == 3 {
			for i := 0; i < util.KClientConcurrentNum; i++ {
				BatchTraceList[pre].TraceMapSlice[i] = nil
			}
			BatchTraceList[pre].Count = 0
			//log.Println("free pos: ", pre)
		}
	}
	if BatchTraceList[pos].TraceMapSlice[0] != nil {
		BatchTraceList[pos].Count++
		if BatchTraceList[pos].Count == 3 {
			for i := 0; i < util.KClientConcurrentNum; i++ {
				BatchTraceList[pos].TraceMapSlice[i] = nil
			}
			BatchTraceList[pos].Count = 0
			//log.Println("free pos: ", pos)
		}
	}
	if BatchTraceList[next].TraceMapSlice[0] != nil {
		BatchTraceList[next].Count++
		if BatchTraceList[next].Count == 3 {
			for i := 0; i < util.KClientConcurrentNum; i++ {
				BatchTraceList[next].TraceMapSlice[i] = nil
			}
			BatchTraceList[next].Count = 0
			//log.Println("free pos: ", next)
		}
	}
	//}

	mm := proto.TraceMap{traceMap}
	bytes, _ := mm.MarshalJSON()
	//bytes, _ := json.Marshal(traceMap)
	return bytes
}

func dealLine(lineChansIndex int) {
	lineCount := 0
	traceMap := make(util.TraceMap)
	wrongTraceSet := mapset.NewSet() //不是用 wrongTraceSet.Clear()
	pos := 0

	for line := range LineChans[lineChansIndex] {
		if len(line) == 0 {
			return
		}
		lineCount++

		//获得 traceId
		firstIndex := strings.Index(string(line), "|")
		if firstIndex == -1 {
			return
		}
		traceId := line[:firstIndex]

		//获得 tags
		lastIndex := strings.LastIndex(string(line), "|")
		if lastIndex == -1 {
			return
		}
		tags := line[lastIndex : len(line)-1]

		if len(tags) > 0 {
			traceMap[util.Bytes2str(traceId)] = append(traceMap[util.Bytes2str(traceId)], line)

			if len(tags) > 8 {
				if bytes.Contains((tags), []byte("error=1")) ||
					(bytes.Contains((tags), []byte("http.status_code=")) &&
						!bytes.Contains((tags), []byte("http.status_code=200"))) {
					wrongTraceSet.Add(util.Bytes2str(traceId))
				}
			}
		}

		if lineCount%BatchSizePerGo == 0 {

		repeat:
			if BatchTraceList[pos].TraceMapSlice[lineChansIndex] == nil {
				BatchTraceList[pos].TraceMapSlice[lineChansIndex] = traceMap
				traceMap = make(util.TraceMap)
			} else { //不为空，说明尚未被消费，需要等待
				time.Sleep(10 * time.Millisecond)
				goto repeat
			}

			pos = (pos + 1) % util.KBatchCount
			batchPos := lineCount/BatchSizePerGo - 1
			go updateWrongTraceId(wrongTraceSet, batchPos)

			wrongTraceSet = mapset.NewSet() //不是用 wrongTraceSet.Clear()
		}
	}

repeat1:
	if BatchTraceList[pos].TraceMapSlice[lineChansIndex] == nil {
		BatchTraceList[pos].TraceMapSlice[lineChansIndex] = traceMap
		traceMap = make(util.TraceMap)
	} else { //不为空，说明尚未被消费，需要等待
		time.Sleep(10 * time.Millisecond)
		goto repeat1
	}

	batchPos := lineCount / BatchSizePerGo
	updateWrongTraceId(wrongTraceSet, batchPos)
	LineProcessWg.Done()
}

func ProcessTraceData() {
	traceDataPath := getTraceDataPath()
	if len(traceDataPath) == 0 {
		log.Println("traceDataPath is empty")
		return
	}

	//slice块的长度为 64MB + 1000
	//开启多个协程负责并行读取，每个协程读取的范围为64MB，注意开始和最后的换行符切割
	//每个协程有一个channel负责存储，每个channel的长度目前定为2

	const chunkSize int = 1 * 1024 * 1024
	const downloadGoCount int = 1
	const bufferCount int = 100
	//begin := time.Now()

	downLoadChans := make([]chan *bytes.Buffer, downloadGoCount)
	for i := 0; i < downloadGoCount; i++ {
		downLoadChans[i] = make(chan *bytes.Buffer, bufferCount)
	}

	getBufferChans := make([]chan *bytes.Buffer, downloadGoCount)
	for i := 0; i < downloadGoCount; i++ {
		getBufferChans[i] = make(chan *bytes.Buffer, bufferCount)
		for j := 0; j < bufferCount; j++ {
			buffer := bytes.NewBuffer(nil)
			buffer.Grow(chunkSize)
			getBufferChans[i] <- buffer
		}
	}

	for i := 0; i < downloadGoCount; i++ {
		go func(index int) {
			for j := 0; true; j++ {
				buffer := <-getBufferChans[index]
				req, _ := http.NewRequest("GET", traceDataPath, nil)
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunkSize*(index+downloadGoCount*j), chunkSize*(index+1+downloadGoCount*j)-1))

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					panic("query topic failed" + err.Error())
				}
				defer resp.Body.Close()

				if resp.StatusCode != 206 {
					//fmt.Println("!= 206")
					//close(getBufferChans[index])
					close(downLoadChans[index])
					break
				}

				buffer.Reset()
				io.Copy(buffer, resp.Body)
				downLoadChans[index] <- buffer
			}
		}(i)
	}

	var lineCount int = 0
	index := 0
	var remaindSlice []byte
	for true {
		buffer, ok := <-downLoadChans[index%downloadGoCount]
		if ok == false {
			break
		}

		if remaindSlice != nil && len(remaindSlice) > 0 {
			line, err := buffer.ReadBytes('\n')
			if err == io.EOF {
				remaindSlice = append(remaindSlice, line...)
				break
			}

			line = append(remaindSlice, line...)

			lineCount++
			LineChans[lineCount%util.KClientConcurrentNum] <- line
		}

		for true {
			line, err := buffer.ReadBytes('\n')
			if err == io.EOF {
				remaindSlice = line
				break
			}

			lineCount++
			LineChans[lineCount%util.KClientConcurrentNum] <- line
		}

		getBufferChans[index%downloadGoCount] <- buffer
		index++
	}

	for i := 0; i < util.KClientConcurrentNum; i++ {
		close(LineChans[i])
	}

	LineProcessWg.Wait()
	notifyFinish()
}

//向 backendprocess 更新错误的 traceId
func updateWrongTraceId(wrongTraceSet mapset.Set, batchPos int) {
	traceIds := make([]string, 0, 1024)
	for tmp := range wrongTraceSet.Iter() {
		traceIds = append(traceIds, tmp.(string))
	}

	ss := proto.TraceIds{traceIds}
	jsonStr, _ := ss.MarshalJSON()

	//jsonStr, _ := json.Marshal(wrongTraceSet)
	if len(jsonStr) <= 0 {
		return
	}

	data := make(url.Values)
	data.Add("traceIdListJson", string(jsonStr))
	data.Add("batchPos", strconv.Itoa(batchPos))
	resp, err := http.PostForm("http://localhost:8002/setWrongTraceId", data)
	if err == nil {
		ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	} else {
		log.Fatalln(err)
	}
}

//向 backendprocess 通知 clientprocess 执行完毕
func notifyFinish() {
	req, err := http.NewRequest("GET", "http://localhost:"+util.KBackendProcessPort+"/finish", nil)
	if err != nil {
		panic(err)
	}

	resp, err := util.CallHTTP(req)
	ioutil.ReadAll(resp.Body)
	if err == nil {
		defer resp.Body.Close()
	} else {
		log.Fatalln(err)
	}
}

//根据 clientprocess 获得 trace-data 路径
func getTraceDataPath() string {
	switch util.KListenPort {
	case util.KClientProcessPort1:
		return "http://localhost:" + util.KTraceDataPort + "/trace1.data"
	case util.KClientProcessPort2:
		return "http://localhost:" + util.KTraceDataPort + "/trace2.data"
	default:
		return ""
	}
}
