package main

import (
	"flag"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"log"
	"net"
	"net/http"
	"os"
	"tail-based-sampling/src/backendprocess"
	"tail-based-sampling/src/clientprocess"
	"tail-based-sampling/src/util"
	"time"
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

	http.DefaultClient.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxConnsPerHost:       1000,
		MaxIdleConnsPerHost:   1000,
		DisableKeepAlives:     false,
	}

	http.DefaultTransport.(*http.Transport).MaxIdleConns = 0
	http.DefaultTransport.(*http.Transport).MaxConnsPerHost = 1000
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	//r.Use(gin.Logger(), gin.Recovery())
	r.Use(gin.Recovery())
	pprof.Register(r) // 性能

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
