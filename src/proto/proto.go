package proto

import "tail-based-sampling/src/util"

//easyjson:json
type TraceIds struct {
	TraceIds []string `json:"trace_ids"`
}

//easyjson:json
type TraceMap struct {
	Traces util.TraceMap `json:"trace_map"`
}

//easyjson.exe -all proto.go
