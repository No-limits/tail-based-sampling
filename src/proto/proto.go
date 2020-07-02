package proto

import "tail-based-sampling/src/util"

//easyjson:json
type TraceIds struct {
	TraceIds []util.TraceId `json:"trace_ids"`
}

//easyjson:json
type TraceMap struct {
	Traces util.TraceMap `json:"trace_map"`
}

//easyjson.exe -all proto.go
