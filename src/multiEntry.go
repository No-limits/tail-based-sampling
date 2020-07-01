package main

import (
	"flag"

	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"os"
	//_ "net/http/pprof"
	"tail-based-sampling/src/backendprocess"
	"tail-based-sampling/src/clientprocess"
	"tail-based-sampling/src/util"
)

//client process
//1、ready
//2、start
//3、setParameter
//4、readLine from trace-data file
//5、filter wrong trace
//6、setWrongTrace
//7、getWrongTrace

func ready(c *gin.Context) {
	c.String(http.StatusOK, "suc")
}

func start(c *gin.Context) {
	c.String(http.StatusOK, "suc")
}

//设置参数，默认情况下，trace-data 与 sampleing process 在同一端口上
func setParameter(c *gin.Context) {
	port := c.Query("port")
	if len(port) == 0 {
		c.String(http.StatusOK, "invalid port")
		return
	}

	util.KSamplingPort = port

	//本地测试时需要注释掉
	util.KTraceDataPort = port

	//log.Println("KSamplingPort: ", util.KSamplingPort)
	//log.Println("KTraceDataPort: ", util.KTraceDataPort)

	if util.IsClientProcess() {
		go clientprocess.ProcessTraceData()
	} else if util.IsBackendProcess() {
		go backendprocess.Process()
	}

	c.String(http.StatusOK, "suc")
}

func exit(c *gin.Context) {
	os.Exit(0)
}

func main() {
	flag.StringVar(&util.KListenPort, "port", "8000", "Listen Port")
	flag.Parse()

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	//pprof.Register(r) // 性能

	r.GET("/ready", ready)
	r.GET("/start", start)
	r.GET("/setParameter", setParameter)
	r.GET("/exit", exit)
	if util.IsClientProcess() {
		r.POST("/getWrongTrace", clientprocess.GetWrongTrace)
	}
	if util.IsBackendProcess() {
		r.POST("/setWrongTraceId", backendprocess.SetWrongTraceId)
		r.GET("/finish", backendprocess.Finish)
	}
	r.Run("127.0.0.1:" + util.KListenPort)
}
