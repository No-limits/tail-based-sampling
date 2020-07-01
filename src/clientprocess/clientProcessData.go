package clientprocess

import (
	"bufio"
	"encoding/json"
	mapset "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"tail-based-sampling/src/util"
	"time"
)

var BatchTraceList util.TraceMapSlice

func init() {
	BatchTraceList = make(util.TraceMapSlice, util.KBatchCount+1)
}

func GetWrongTrace(c *gin.Context) {
	wrongTraceSetStr := c.PostForm("traceIdList")
	batchPos, _ := strconv.Atoi(c.PostForm("batchPos"))

	//log.Println("wrongTraceSet: ", wrongTraceSetStr)
	//log.Println("batchPos: ", batchPos)

	data := getWrongTracing(wrongTraceSetStr, batchPos)

	c.Writer.Write(util.Str2bytes(data))
}

// RestFul 接口实际调用的本函数，根据 TraceIds 获得所有的日志
func getWrongTracing(wrongTraceSetStr string, batchPos int) string {

	wrongTraceSet := mapset.NewSet()
	json.Unmarshal([]byte(wrongTraceSetStr), &wrongTraceSet)

	pos := batchPos % util.KBatchCount
	//log.Println(" getwrongtracing batchpos:", batchPos, "pos: ", pos)
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
		for traceId := range wrongTraceSet.Iter() {
			if BatchTraceList[pos].TraceMap != nil {
				traceIdStr := util.TraceId(traceId.(string))
				traceMap[traceIdStr] = append(traceMap[traceIdStr], BatchTraceList[pos].TraceMap[traceIdStr]...)
			}
			//if "17ff7cb829358b03" == traceId.(string){
			//	log.Println("", traceMap[util.TraceId(traceId.(string))].Len())
			//	log.Printf("%s",traceMap[util.TraceId(traceId.(string))])
			//}
		}
	}

	getWrongTracingWithBatch(pre)
	getWrongTracingWithBatch(pos)
	getWrongTracingWithBatch(next)

	//if batchPos != 0 {
	if BatchTraceList[pre].TraceMap != nil {
		BatchTraceList[pre].Count++
		if BatchTraceList[pre].Count == 3 || (BatchTraceList[pre].Count == 2 && (batchPos == 1 || batchPos == 0)) {
			BatchTraceList[pre].TraceMap = nil
			BatchTraceList[pre].Count = 0
			//log.Println("free pos: ", pre)
		}
	}
	if BatchTraceList[pos].TraceMap != nil {
		BatchTraceList[pos].Count++
		if BatchTraceList[pos].Count == 3 {
			BatchTraceList[pos].TraceMap = nil
			BatchTraceList[pos].Count = 0
			//log.Println("free pos: ", pos)
		}
	}
	if BatchTraceList[next].TraceMap != nil {
		BatchTraceList[next].Count++
		if BatchTraceList[next].Count == 3 {
			BatchTraceList[next].TraceMap = nil
			BatchTraceList[next].Count = 0
			//log.Println("free pos: ", next)
		}
	}
	//}
	bytes, _ := json.Marshal(traceMap)
	return string(bytes)
}

func ProcessTraceData() {
	traceDataPath := getTraceDataPath()
	if len(traceDataPath) == 0 {
		log.Println("traceDataPath is empty")
		return
	}

	//log.Println("traceDataPath: ", traceDataPath)
	resp, err := http.Get(traceDataPath)
	if err == nil {
		defer resp.Body.Close()
	} else {
		log.Fatalln(err)
	}

	bufReader := bufio.NewReader(resp.Body)

	var lineCount int = 0
	var pos int = 0 //BatchTraceList 中正在操作的 index
	wrongTraceSet := mapset.NewSet()
	traceMap := make(util.TraceMap)

	begin := time.Now()
	for {
		line, err := bufReader.ReadBytes('\n') //传入固定大小数组，可以优化性能？
		if err != nil && err != io.EOF {
			log.Println("bufReader.ReadBytes meet unsolved error")
			panic(err)
		}
		if len(line) == 0 && err == io.EOF {
			break
		}

		lineCount++

		//获得 traceId
		firstIndex := strings.Index(string(line), "|")
		if firstIndex == -1 {
			continue
		}
		traceId := line[:firstIndex]

		//获得 tags
		lastIndex := strings.LastIndex(string(line), "|")
		if lastIndex == -1 {
			continue
		}
		tags := line[lastIndex : len(line)-1]

		if len(tags) > 0 {
			//if _, ok := traceMap[util.TraceId(traceId)]; ok == false{
			//	traceMap[util.TraceId(traceId)] = make(util.SpanSlice, 0, 2048)
			//}

			traceMap[util.TraceId(traceId)] = append(traceMap[util.TraceId(traceId)], line)

			if len(tags) > 8 {
				if strings.Contains(util.Bytes2str(tags), "error=1") ||
					(strings.Contains(util.Bytes2str(tags), "http.status_code=") &&
						!strings.Contains(util.Bytes2str(tags), "http.status_code=200")) {
					wrongTraceSet.Add(util.TraceId(traceId))
				}
			}
		}

		if lineCount%util.KBatchSize == 0 {

			batchPos := lineCount/util.KBatchSize - 1

			//TODO BatchTraceList需要互斥访问
		repeat:
			if BatchTraceList[pos].TraceMap == nil {
				BatchTraceList[pos].TraceMap = traceMap
				traceMap = make(util.TraceMap)
			} else { //不为空，说明尚未被消费，需要等待
				time.Sleep(1 * time.Millisecond)
				//log.Println("pos = ", pos)
				//log.Println(BatchTraceList[0].TraceMap != nil, BatchTraceList[0].Count, BatchTraceList[1].TraceMap != nil, BatchTraceList[1].Count, BatchTraceList[2].TraceMap != nil, BatchTraceList[2].Count)
				goto repeat
			}

			pos = (pos + 1) % util.KBatchCount
			go updateWrongTraceId(wrongTraceSet, batchPos)
			wrongTraceSet = mapset.NewSet() //不是用 wrongTraceSet.Clear()
		}
	}

	log.Printf("%v\n", time.Since(begin))

	//if wrongTraceSet.Cardinality() > 0 {
	batchPos := lineCount / util.KBatchSize
repeat2:
	if BatchTraceList[pos].TraceMap == nil {
		BatchTraceList[pos].TraceMap = traceMap
	} else { //不为空，说明尚未被消费，需要等待
		time.Sleep(1 * time.Millisecond)
		goto repeat2
	}

	//log.Printf("%d  %s\n", batchPos, traceMap["14fd002645313053"])
	updateWrongTraceId(wrongTraceSet, batchPos)
	//}

	notifyFinish()

	//os.Exit(0)
}

//向 backendprocess 更新错误的 traceId
func updateWrongTraceId(wrongTraceSet mapset.Set, batchPos int) {
	jsonStr, _ := json.Marshal(wrongTraceSet)
	if len(jsonStr) <= 0 {
		return
	}

	data := make(url.Values)
	data.Add("traceIdListJson", string(jsonStr))
	data.Add("batchPos", strconv.Itoa(batchPos))
	resp, err := http.PostForm("http://localhost:8002/setWrongTraceId", data)
	//req, _ := http.NewRequest("POST", "http://localhost:8002/setWrongTraceId", strings.NewReader(data.Encode()))
	//resp, _ := util.CallHTTP(req)
	if err == nil {
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
