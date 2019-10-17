package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	file, _ := os.OpenFile(outFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0755)
	enc := json.NewEncoder(file)
	defer file.Close()
	midRes := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inFile, err := os.OpenFile(reduceName(jobName, i, reduceTask), os.O_RDONLY, 0755)
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			err = dec.Decode(&kv) //(data, &kv)
			if err != nil {
				break
			}
			midRes[kv.Key] = append(midRes[kv.Key], kv.Value)
		}
	}
	contents := make([]KeyValue, 0, len(midRes))

	for key := range midRes {
		contents = append(contents, KeyValue{Key: key, Value: ""})
	}

	sort.Sort(ByKey(contents))
	for _, kv := range contents {
		enc.Encode(KeyValue{kv.Key, reduceF(kv.Key, midRes[kv.Key])})
	}
}
