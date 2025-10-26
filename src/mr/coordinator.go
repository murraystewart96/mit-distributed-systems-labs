package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	taskTimeout = 10 * time.Second
)

type Coordinator struct {
	// Your definitions here.
	idleMapTasks        []*Task // maybe have two queue - one for idle and one for active
	activeMapTask       map[uuid.UUID]*Task
	totalMapTasks       int
	numCompleteMapTasks int

	idleReduceTasks        []*Task
	activeReduceTask       map[uuid.UUID]*Task
	totalReduceTasks       int
	numCompleteReduceTasks int

	doneTaskChans map[uuid.UUID]chan struct{}
	jobComplete   bool

	mu   *sync.RWMutex
	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Create done channel for task
	c.doneTaskChans[args.WorkerID] = make(chan struct{})

	// Assign map task if any map tasks are remaining

	c.mu.Lock()
	switch {
	case len(c.idleMapTasks) > 0:
		// Assign task
		mapTask := c.idleMapTasks[len(c.idleMapTasks)-1]
		reply.Task = *mapTask
		c.idleMapTasks = c.idleMapTasks[:len(c.idleMapTasks)-1]

		// Mark task as active
		mapTask.State = ACTIVE
		mapTask.WorkerID = args.WorkerID
		c.activeMapTask[args.WorkerID] = mapTask

		// Start task timer
		go func() {

		}()

	case c.numCompleteMapTasks < c.totalMapTasks:
		// Waiting for map tasks to complete
		// sync.cond
	case len(c.idleReduceTasks) > 0:
		// Assign task

	case c.numCompleteReduceTasks < c.totalReduceTasks:
		// Wait for reduce tasks to complete

	default:
		// Job is done -> exit program
		c.jobComplete = true
	}

	c.mu.Unlock()
	return nil

	// When a worker is deemed to have failed what do we do?
	// first of all how do we track this?
	// i imagine when we assign a task we will spawn a goroutine that starts a timer.
	// When the task is complete we will cancel this timer
	// if the timer finished before the task completes we mark the task as IDLE again

	// What does the goroutine monitoring the timeout of a task need?
	// The worker should notify the co-ordinator when it is finished with a task
	// When you start a go-routine it should have a channel that is mapped to the worker ID

	// When the task is being marked as complete you could close the channel and delete the channel from the map
	// so pretty much we are saying everytime a task is assigned we create a channel for that task (using worker ID)
	// when the task is complete we are done with that channel, we close which signals for the timer routine to stop, then we delete the channel
	// maybe dont even delete the channel, just assign a new channel every time a worker asks for a task. the old one will be gargage collected

	// when you complete a map task remove it from active
}

func (c *Coordinator) taskTimer(workerID uuid.UUID) {
	done := c.doneTaskChans[workerID]
	timer := time.NewTimer(taskTimeout)

	for {
		select {
		case <-timer.C:
			// Task has timed out - make task available for other workers
			c.mu.Lock()
			task := c.activeTasks[workerID]
			task.State = IDLE
			c.mu.Unlock()

			return
		case <-done:
			// Task is complete
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.jobComplete
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:               &sync.RWMutex{},
		cond:             &sync.Cond{},
		totalMapTasks:    len(files),
		totalReduceTasks: nReduce,
	}

	// Your code here.

	// Create map tasks from input files
	c.idleMapTasks = make([]*Task, len(files))
	for i, file := range files {
		task := &Task{
			Type:      MAP,
			State:     IDLE,
			InputFile: file,
		}

		c.idleMapTasks[i] = task
	}

	c.server()
	return &c
}
