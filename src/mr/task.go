package mr

import "github.com/google/uuid"

type Type int
type State int

const (
	// Task types
	MAP Type = iota
	REDUCE

	// Task states
	IDLE = iota
	ACTIVE
	COMPLETED
)

type Task struct {
	Type      Type
	State     State
	WorkerID  uuid.UUID // for active tasks
	InputFile string
}
