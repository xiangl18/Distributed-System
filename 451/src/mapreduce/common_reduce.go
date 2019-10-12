package mapreduce

import (
	"encoding/json"
	"log"
	"sort"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	var kvPairs map[string][]string
	kvPairs = make(map[string][]string)
	for i := 0; i < nMap; i++ {
		temp := reduceName(jobName, i, reduceTaskNumber)
		file, e := os.Open(temp)
		if e != nil {
			log.Printf("Failed when opening temp file: %s", temp)
			return
		}
		decoder := json.NewDecoder(file)
		var kv KeyValue
		e = decoder.Decode(&kv)
		for e == nil {
			_, appeared := kvPairs[kv.Key]
			if !appeared {
				kvPairs[kv.Key] = []string{}
			}
			kvPairs[kv.Key] = append(kvPairs[kv.Key], kv.Value)
			e = decoder.Decode(&kv)
		}
		file.Close()
	}

	var tempKeys []string
	for k := range kvPairs {
		tempKeys = append(tempKeys, k)
	}
	sort.Strings(tempKeys)
	outN := mergeName(jobName, reduceTaskNumber)
	output, e := os.Create(outN)
	if e != nil {
		log.Printf("Failed when creating reduce file: %s", outN)
		return 
	}
	encoder := json.NewEncoder(output)
	for _, k := range tempKeys {
		value := reduceF(k, kvPairs[k])
		e = encoder.Encode((KeyValue{k, value}))
		if e != nil {
			log.Printf("Failed whne writing [key: %s] to file %s", k, outN)
		}
	}
}
