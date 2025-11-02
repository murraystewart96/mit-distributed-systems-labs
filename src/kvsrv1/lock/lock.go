package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey  string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l
	lk.clientID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// Get lock state
		lockState, version, rpcErr := lk.ck.Get(lk.lockKey)
		if rpcErr != rpc.ErrNoKey {
			fmt.Printf("rpc call error: %s", rpcErr)
			return
		}

		// Acquire lock if its free or first time being acquired
		if lockState == "FREE" || rpcErr == rpc.ErrNoKey {
			// Write to lock
			rpcErr := lk.ck.Put(lk.lockKey, lk.clientID, version+1)
			if rpcErr == rpc.OK {
				return
			} else if rpcErr != rpc.ErrVersion || rpcErr != rpc.ErrMaybe {
				fmt.Printf("rpc call error: %s", rpcErr)
				return
			}
		}

		// Lock unavailable - try again
		time.Sleep(300 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	// Your code here

	// Get lock state
	lockState, version, rpcErr := lk.ck.Get(lk.lockKey)
	if rpcErr != rpc.ErrNoKey {
		fmt.Printf("rpc call error: %s", rpcErr)
		return
	}

	// If acquired free lock
	if lockState == lk.clientID {
		// Write to lock
		rpcErr := lk.ck.Put(lk.lockKey, "FREE", version+1)
		if rpcErr != rpc.OK {
			fmt.Printf("rpc call error: %s", rpcErr)
			return
		}
	}
}
