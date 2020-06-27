package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
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

func ready(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("suc"))
}

func start(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("suc"))
}

//设置参数，默认情况下，trace-data 与 sampleing process 在同一端口上
func setParameter(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	port := query.Get("port")
	if len(port) == 0 {
		w.Write([]byte("invalid port"))
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

	w.Write([]byte("suc"))
}

func exit(w http.ResponseWriter, r *http.Request) {
	os.Exit(0)
}

func main() {
	flag.StringVar(&util.KListenPort, "port", "8000", "Listen Port")
	flag.Parse()

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	http.HandleFunc("/ready", ready)
	http.HandleFunc("/start", start)
	http.HandleFunc("/setParameter", setParameter)
	http.HandleFunc("/exit", exit)
	if util.IsClientProcess() {
		http.HandleFunc("/getWrongTrace", clientprocess.GetWrongTrace)
	}
	if util.IsBackendProcess() {
		http.HandleFunc("/setWrongTraceId", backendprocess.SetWrongTraceId)
		http.HandleFunc("/finish", backendprocess.Finish)
	}
	err := http.ListenAndServe(fmt.Sprint("127.0.0.1:", util.KListenPort), nil)
	if err != nil {
		panic(err)
	}
}
