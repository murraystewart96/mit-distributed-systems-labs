package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

const TaskInterval = 1 * time.Second // Interval before asking for next task

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := uuid.New()

	// Your worker implementation here.
	for {
		time.Sleep(TaskInterval)

		// Get Task from coordinator
		task, err := GetTask(workerID)
		if err != nil {
			fmt.Printf("failed to get task: %s", err.Error())
			continue
		}

		switch task.Type {
		case MAP:
			intKvs := []KeyValue{}

			file, err := os.Open(task.InputFile)
			if err != nil {
				log.Fatalf("cannot open %v", task.InputFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.InputFile)
			}
			file.Close()
			kvs := mapf(task.InputFile, string(content))
			intKvs = append(intKvs, kvs...)

			// Create buffered partition encoders
			encoders := make(map[int]*json.Encoder)
			files := make([]*os.File, task.NReduce)
			buffers := make([]*bufio.Writer, task.NReduce)

			for i := range task.NReduce {
				intFilename := fmt.Sprintf("mr-%d-%d", task.ID, i)

				files[i], err = os.Create(intFilename)
				if err != nil {
					fmt.Printf("failed to open file (%s): %s", intFilename, err.Error())

					continue // SKIP TASK
				}
				buffers[i] = bufio.NewWriter(files[i])
				encoders[i] = json.NewEncoder(buffers[i])
			}

			// Partition intermediate values
			for _, kv := range intKvs {
				partition := ihash(kv.Key) % task.NReduce
				w := encoders[partition]

				if err := w.Encode(&kv); err != nil {
					fmt.Printf("failed to encode KV %v: %s", kv, err.Error())
				}
			}

			// Flush buffers and close files
			for i := range task.NReduce {
				buffers[i].Flush()
				files[i].Close()
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func GetTask(workerID uuid.UUID) (Task, error) {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}

	err := call("Coordinator.GetTask", &args, &reply)
	if err != nil {
		return Task{}, fmt.Errorf("rpc failed: %w", err)
	}

	return reply.Task, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err != nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
