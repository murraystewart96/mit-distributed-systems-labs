package mr

import (
	"fmt"
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

	idleMapTasks        []*Task
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
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create done channel for task
	c.doneTaskChans[args.WorkerID] = make(chan struct{})

	switch {
	case len(c.idleMapTasks) > 0:
		reply.Task = c.assignTask(args.WorkerID, MAP)

	case c.numCompleteMapTasks < c.totalMapTasks:
		// Waiting for all map tasks to complete or failed task to become available
		for c.numCompleteMapTasks != c.totalMapTasks || len(c.idleMapTasks) != 0 {
			c.cond.Wait()
		}

		if len(c.idleMapTasks) > 0 {
			// Assign map task
			reply.Task = c.assignTask(args.WorkerID, MAP)
		} else if len(c.idleReduceTasks) > 0 {
			// All map tasks complete - assign reduce task
			reply.Task = c.assignTask(args.WorkerID, REDUCE)
		}

	case len(c.idleReduceTasks) > 0:
		reply.Task = c.assignTask(args.WorkerID, REDUCE)

	case c.numCompleteReduceTasks < c.totalReduceTasks:
		// Waiting for all reduce tasks to complete or failed task to become available
		for c.numCompleteReduceTasks != c.totalReduceTasks || len(c.idleReduceTasks) != 0 {
			c.cond.Wait()
		}

		if len(c.idleReduceTasks) > 0 {
			// Assign reduce task
			reply.Task = c.assignTask(args.WorkerID, REDUCE)
		}

	default:
		// Job is done -> exit program
		c.jobComplete = true
	}

	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case MAP:
		_, found := c.activeMapTask[args.WorkerID]
		if !found {
			return fmt.Errorf("unknown map task completed by worker - %s", args.WorkerID.String())
		}

		delete(c.activeMapTask, args.WorkerID)

		c.numCompleteMapTasks++
		if c.numCompleteMapTasks == c.totalMapTasks {
			c.cond.Broadcast() // Wake up any waiting GetTask requests
		}

		// broadcast
	case REDUCE:
		_, found := c.activeReduceTask[args.WorkerID]
		if !found {
			return fmt.Errorf("unknown reduce task completed by worker - %s", args.WorkerID.String())
		}

		delete(c.activeReduceTask, args.WorkerID)

		c.numCompleteReduceTasks++
		if c.numCompleteReduceTasks == c.totalReduceTasks {
			c.cond.Broadcast() // Wake up any waiting GetTask requests
		}
	}

	// Terminate task timer by closing done channel
	done := c.doneTaskChans[args.WorkerID]
	close(done)
	delete(c.doneTaskChans, args.WorkerID)

	return nil
}

func (c *Coordinator) taskTimer(task *Task, done chan struct{}) {
	timer := time.NewTimer(taskTimeout)

	for {
		select {
		case <-timer.C:
			switch task.Type {
			case MAP:
				// Task has timed out - make task available for other workers
				c.mu.Lock()
				task.State = IDLE
				c.idleMapTasks = append(c.idleMapTasks, task)

				// Remove failed worker's active task
				delete(c.activeMapTask, task.WorkerID)
				c.mu.Unlock()
			case REDUCE:
				// Task has timed out - make task available for other workers
				c.mu.Lock()
				task.State = IDLE
				c.idleReduceTasks = append(c.idleReduceTasks, task)

				// Remove failed worker's active task
				delete(c.activeReduceTask, task.WorkerID)
				c.mu.Unlock()
			}

			return
		case <-done:
			// Task is complete
			return
		}
	}
}

func (c *Coordinator) assignTask(workerID uuid.UUID, taskType Type) Task {
	var replyTask Task

	switch taskType {
	case MAP:
		// Assign task
		assignedTask := c.idleMapTasks[len(c.idleMapTasks)-1]
		replyTask = *assignedTask

		c.idleMapTasks = c.idleMapTasks[:len(c.idleMapTasks)-1]

		// Mark task as active
		assignedTask.State = ACTIVE
		assignedTask.WorkerID = workerID
		c.activeMapTask[workerID] = assignedTask

		// Start task timer
		go c.taskTimer(assignedTask, c.doneTaskChans[workerID])
	case REDUCE:
		// Assign task
		assignedTask := c.idleReduceTasks[len(c.idleReduceTasks)-1]
		replyTask = *assignedTask

		c.idleReduceTasks = c.idleReduceTasks[:len(c.idleReduceTasks)-1]

		// Mark task as active
		assignedTask.State = ACTIVE
		assignedTask.WorkerID = workerID
		c.activeReduceTask[workerID] = assignedTask

		// Start task timer
		go c.taskTimer(assignedTask, c.doneTaskChans[workerID])
	}

	return replyTask
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
	mu := &sync.RWMutex{}
	c := Coordinator{
		mu:               mu,
		cond:             sync.NewCond(mu),
		totalMapTasks:    len(files),
		totalReduceTasks: nReduce,
		activeMapTask:    make(map[uuid.UUID]*Task),
		activeReduceTask: map[uuid.UUID]*Task{},
		doneTaskChans:    make(map[uuid.UUID]chan struct{}),
	}

	// Your code here.

	// Create map tasks from input files
	c.idleMapTasks = make([]*Task, len(files))
	for i, file := range files {
		task := &Task{
			ID:        i,
			Type:      MAP,
			State:     IDLE,
			InputFile: file,
			NReduce:   nReduce,
		}

		c.idleMapTasks[i] = task
	}

	// Create reduce tasks
	c.idleReduceTasks = make([]*Task, nReduce)
	for i := range nReduce {
		task := &Task{
			ID:   i,
			Type: REDUCE,
		}
	}

	c.server()
	return &c
}
