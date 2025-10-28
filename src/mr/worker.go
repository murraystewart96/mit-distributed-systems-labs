package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

const TaskInterval = 1 * time.Second // Interval before asking for next task

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetTask(workerID uuid.UUID) (GetTaskReply, error) {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}

	err := call("Coordinator.GetTask", &args, &reply)
	if err != nil {
		return GetTaskReply{}, fmt.Errorf("rpc failed: %w", err)
	}

	return reply, nil
}

func TaskComplete(workerID uuid.UUID, taskType Type) error {
	args := TaskCompleteArgs{WorkerID: workerID, Type: taskType}
	reply := TaskCompleteReply{}

	err := call("Coordinator.TaskComplete", &args, &reply)
	if err != nil {
		return fmt.Errorf("rpc failed: %w", err)
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := uuid.New()

	// Your worker implementation here.
	for {
		time.Sleep(TaskInterval) // small sleep as to not hammer the coordinator

		// Get Task from coordinator
		taskReply, err := GetTask(workerID)
		if err != nil {
			fmt.Printf("failed to get task: %s", err.Error())
			continue
		}

		task := taskReply.Task

		switch task.Type {
		case MAP:
			// Map input to int key values
			intKvs := mapKvs(&task, mapf)

			// Write key values - partitioned by key
			writeIntKeyValues(task.ID, taskReply.NReduce, intKvs)

			// Inform coordinator that task is complete
			if err := TaskComplete(workerID, task.Type); err != nil {
				fmt.Printf("failed to mark task as complete: %s", err.Error())
			}

		case REDUCE:
			// Read and sort key values
			intKvs := readIntKeyValues(&task, taskReply.NMap)
			sort.Sort(ByKey(intKvs))

			// Reduce
			reduceKvs(&task, intKvs, reducef)

			// Inform coordinator task is complete
			if err := TaskComplete(workerID, task.Type); err != nil {
				fmt.Printf("failed to mark task as complete: %s", err.Error())
			}
		}
	}
}

// ** Map helpers ***

func mapKvs(task *Task, mapFunc func(string, string) []KeyValue) []KeyValue {
	intKvs := []KeyValue{}

	file, err := os.Open(task.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", task.InputFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFile)
	}
	file.Close()
	kvs := mapFunc(task.InputFile, string(content))
	intKvs = append(intKvs, kvs...)

	return intKvs
}

func writeIntKeyValues(taskID, nReduce int, intKvs []KeyValue) {
	// Create buffered partition encoders
	encoders := make(map[int]*json.Encoder)
	files := make([]*os.File, nReduce)
	buffers := make([]*bufio.Writer, nReduce)

	var err error
	for i := range nReduce {
		intFilename := fmt.Sprintf("mr-%d-%d", taskID, i)

		files[i], err = os.Create(intFilename)
		if err != nil {
			log.Fatalf("failed to open file (%s): %s", intFilename, err.Error())
		}
		buffers[i] = bufio.NewWriter(files[i])
		encoders[i] = json.NewEncoder(buffers[i])
	}

	// Partition intermediate values
	for _, kv := range intKvs {
		partition := ihash(kv.Key) % nReduce
		w := encoders[partition]

		if err := w.Encode(&kv); err != nil {
			fmt.Printf("failed to encode KV %v: %s", kv, err.Error())
		}
	}

	// Flush buffers and close files
	for i := range nReduce {
		buffers[i].Flush()
		files[i].Close()
	}
}

// *** Reduce helpers ***

func readIntKeyValues(task *Task, nMap int) []KeyValue {
	intKvs := []KeyValue{}

	for i := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", i, task.ID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", task.InputFile)
		}

		decoder := json.NewDecoder(file)

		for {
			kv := KeyValue{}
			if err := decoder.Decode(&kv); err != nil {
				if err.Error() == "EOF" {
					break
				}
				fmt.Printf("failed to decode key value: %s", err.Error())
			}

			intKvs = append(intKvs, kv)
		}

		file.Close()
	}

	return intKvs
}

func reduceKvs(task *Task, intKvs []KeyValue, reduceFunc func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%d", task.ID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}

	i := 0
	for i < len(intKvs) {
		j := i + 1
		for j < len(intKvs) && intKvs[j].Key == intKvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intKvs[k].Value)
		}
		output := reduceFunc(intKvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intKvs[i].Key, output)

		i = j
	}

	ofile.Close()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return nil
	}

	return fmt.Errorf("rpc failed: %w", err)
}
