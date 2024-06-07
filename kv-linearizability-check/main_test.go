package main

import (
    "encoding/json"
    "github.com/anishathalye/porcupine"
	"io/ioutil"
    "log"
	//"os"
	"reflect"
	"testing"
)

type JSONLogEntry struct {
    End  int64         `json:"end"`
    Start  int64        `json:"start"`
    Value []interface{} `json:"value"`
}

type kvInput struct {
	op    uint8 // 0 => get, 1 => put
	key   string
	value string
}

type kvOutput struct {
	value string
}

func cloneMap(m map[string]string) map[string]string {
	m2 := make(map[string]string)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

var kvNoPartitionModel = porcupine.Model{
	Init: func() interface{} {
		return make(map[string]string)
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(kvInput)
		out := output.(kvOutput)
		st := state.(map[string]string)
		if inp.op == 0 {
			// get
			return out.value == st[inp.key], state
		} else if inp.op == 1 {
			// put
			st2 := cloneMap(st)
			st2[inp.key] = inp.value
			return true, st2
		}
		return false, state
	},
	Equal: func(state1, state2 interface{}) bool {
		return reflect.DeepEqual(state1, state2)
	},
}

func TestRegisterModel(t *testing.T) {
	// Read the JSON log file produced by your C++ KVS server
    data, err := ioutil.ReadFile("../history.json")
    if err != nil {
        log.Fatalf("Failed to read history file: %v", err)
    }

    var logEntries []JSONLogEntry
    err = json.Unmarshal(data, &logEntries)
    if err != nil {
        log.Fatalf("Failed to unmarshal history: %v", err)
    }

    // Convert JSONLogEntry to kvInput and kvOutput
	var ops []porcupine.Operation

	i := 0
	for _, entry := range logEntries {
        var input kvInput
        var output kvOutput
		if len(entry.Value) > 0 {
			op, ok := entry.Value[0].(string)
            if !ok {
                log.Fatalf("Unexpected type for op: %v", entry.Value[0])
            }

			switch op {
			case "GET":
				input.op = 0
				input.key, _ = entry.Value[1].(string)
				output.value, _ = entry.Value[2].(string)
			case "PUT":
				input.op = 1
				input.key, _ = entry.Value[1].(string)
				input.value, _ = entry.Value[2].(string)
			default:
				log.Fatalf("Unknown operation: %s", op)
			}

			var pop porcupine.Operation
			pop.ClientId = i
			pop.Input = input
			pop.Call = entry.Start
			pop.Output = output
			pop.Return = entry.End
			ops = append(ops, pop)

			i = i + 1
		}
	}

	res := porcupine.CheckOperations(kvNoPartitionModel, ops)
	//res, info := porcupine.CheckOperationsVerbose(kvNoPartitionModel, ops, 0)
	if res != true {
	//if res != "ok" {
		t.Fatal("expected operations to be linearizable")
	}

	/*res1 := porcupine.Visualize(kvNoPartitionModel, info, os.Stdout)
	if res1 != nil {
		t.Fatal("expected operations to be linearizable")
	}*/
}