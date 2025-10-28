package mr

import "github.com/google/uuid"

// ***** MY CODE START *****

type Type int
type State int

const (
	// Task types
	MAP Type = iota
	REDUCE

	// Task states
	IDLE State = iota
	ACTIVE
	COMPLETED
)

type Task struct {
	ID        int
	Type      Type
	State     State
	WorkerID  uuid.UUID // for active tasks
	InputFile string    // only used for map tasks
}

// ***** MY CODE END *****
