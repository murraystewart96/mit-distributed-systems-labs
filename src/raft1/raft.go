package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	"github.com/rs/zerolog/log"
)

type State int

const (
	LEADER State = iota
	CANDIDATE
	FOLLOWER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        *sync.RWMutex       // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	heartbeatChan chan struct{}
	applyCh       chan raftapi.ApplyMsg
	cond          *sync.Cond

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state on all servers
	state       State
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int
}

type Log struct {
	Term    int
	Command any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	// Your code here (3A).
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int // Term of conflicting entry
	XIndex int // Index of first entry with XTerm
	XLen   int // Length of follower log
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).

	rf.mu.RLock()

	// if commit index is higher commit up to commit index (what if log doesnt have all commited entries?)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(len(rf.log)), float64(args.LeaderCommit)))

		// Signal to commit log entries
		rf.cond.Signal()
	}

	reply.Term = rf.currentTerm

	rf.mu.Unlock()

	if len(args.Entries) == 0 {
		// Leader heartbeat

		// Notify heartbeat chan to reset election timer
		rf.heartbeatChan <- struct{}{}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if args.Term >= rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		return
	}

	// Apply log consistency check and append if successful
	lastLogTerm := rf.log[args.PrevLogIndex].Term

	// Check if entry exists at prevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		// Entry doesn't exist
		reply.XLen = len(rf.log)
		reply.XTerm = -1

	} else if lastLogTerm != args.PrevLogTerm {
		// Mismatching log terms
		reply.XTerm = lastLogTerm

		// Get first index of that term in log
		firstIndex := args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != lastLogTerm {
				break
			}
			firstIndex = i
		}

		reply.XIndex = firstIndex
	} else {
		// Matching log terms

		// Truncate log in case of unwanted entries
		rf.log = rf.log[:args.PrevLogIndex+1]

		// Append leader's new entries
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...) // Come back to this later to address out of order appendEntries

		reply.Success = true
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// Reject stale vote request
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || args.CandidateID == rf.votedFor {
		// Check if candidate's log is at least as up-to-date
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term

		logIsUpToDate := false
		if args.LastLogTerm > lastLogTerm {
			logIsUpToDate = true
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			logIsUpToDate = true
		}

		if logIsUpToDate {
			log.Info().Msgf("[%d] voted for server %d", rf.me, args.CandidateID)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.state == LEADER

	if !isLeader {
		return 0, 0, false
	}

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker runs an election timer. The timer is reset whenever a leader heartbeat is received.
// If the timer expires and the server is a FOLLOWER the server starts an election - at which point the select will block until
// the election has finished or a leader heartbeat has been received.
func (rf *Raft) ticker() {
	electionTimer := time.NewTimer(randTimeout())
	electionDone := make(chan struct{})

	for rf.killed() == false {
		// Your code here (3A)
		select {
		case <-electionTimer.C:
			// timeout reached -> start election
			rf.mu.RLock()

			if rf.state == FOLLOWER {
				go rf.startElection(electionDone)
			}

			rf.mu.RUnlock()

		case <-rf.heartbeatChan:
			// Leader is alive -> reset election timeout (still follower)
			electionTimer.Reset(randTimeout())

		case <-electionDone:
			electionTimer.Reset(randTimeout())
		}
	}
}

func (rf *Raft) logCommitWorker() {
	for rf.killed() {
		log.Info().Msgf("[%d] Committing logs from [%d] to [%d]", rf.me, rf.lastApplied, rf.commitIndex)

		rf.mu.Lock()
		rf.cond.Wait()

		// Commit logs
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := raftapi.ApplyMsg{
				Command:      rf.log[i].Command,
				CommandValid: true,
				CommandIndex: i,
			}
			rf.applyCh <- applyMsg
		}

		rf.mu.Lock()
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}
}

// startElection makes the server a candidate and requests votes from its peers.
// There are 3 possible outcomes from the election
// - server wins election and becomes leader - start heartbeat and exit election
// - timeout and server has become follower - exit election
// - timeout and server is still candidate - start new election
func (rf *Raft) startElection(done chan struct{}) {
	// Become candidate
	rf.mu.Lock()
	rf.state = CANDIDATE
	isCandidate := true
	rf.mu.Unlock()

	timeout := randTimeout()
	timer := time.NewTimer(timeout)

	// While candidate
	for isCandidate && !rf.killed() {
		rf.mu.Lock()

		// Increment current term and vote for self
		rf.currentTerm++
		rf.votedFor = rf.me

		log.Info().Msgf("[%d] starting election for term %d", rf.me, rf.currentTerm)

		rf.mu.Unlock()

		// Request votes for this election
		votes := 1
		voteCh := make(chan struct{})
		rf.requestVotes(voteCh)

		// Wait for votes or timeout
		electionOver := false
		for !electionOver {
			select {
			case <-timer.C:
				electionOver = true

				rf.mu.Lock()
				rf.votedFor = -1
				isCandidate = rf.state == CANDIDATE

				rf.mu.Unlock()

				timer.Reset(randTimeout())
			case <-voteCh:
				rf.mu.Lock()
				votes++

				if votes > (len(rf.peers)/2) && rf.state == CANDIDATE {
					// Candidate has won election
					log.Info().Msgf("[%d] won election - changing to leader", rf.me)

					rf.state = LEADER
					isCandidate = false

					// Start sending heartbeats
					go rf.startHeartbeat()

					electionOver = true
					rf.votedFor = -1

					// Init followers nextIndices
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.lastApplied + 1
					}
				}

				rf.mu.Unlock()
			}
		}

		if electionOver && !isCandidate {
			done <- struct{}{}
		}
	}
}

func (rf *Raft) requestVotes(voteCh chan struct{}) {
	// Request vote from other servers
	for server := range rf.peers {
		if server != rf.me {
			rf.mu.RLock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: rf.lastApplied,
				LastLogTerm:  rf.log[rf.lastApplied].Term,
			}
			rf.mu.RUnlock()

			reply := &RequestVoteReply{}

			// Send out vote requests
			go func(server int, voteCh chan struct{}) {
				ok := rf.sendRequestVote(server, args, reply)
				if !ok {
					log.Warn().Msgf("[%d] rpc request vote failed to server: %d", rf.me, server)
				}

				if reply.VoteGranted {
					voteCh <- struct{}{} // receive vote
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
				}
			}(server, voteCh)
		}
	}
}

func (rf *Raft) appendEntries(command interface{}) {
	// Append to leader's log first
	rf.log = append(rf.log, Log{
		Term:    rf.currentTerm,
		Command: command,
	})

	rf.lastApplied = len(rf.log) - 1
	postAppendLength := len(rf.log)

	// Channel for replication acknowledgements
	ackCh := make(chan struct{})

	// Send latest logs to each server
	for server := range rf.peers {
		if server != rf.me {
			nextIndex := rf.nextIndex[server]

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.currentTerm,
				Entries:      rf.log[nextIndex:],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}

			go func(logLength int, ackCh chan struct{}) {
				success := false
				for !success {
					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
						log.Warn().Msgf("rpc append entries failed for [%d]", server)
					}

					// Confirm we are still leader
					if reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						rf.currentTerm = args.Term
						rf.votedFor = -1

						return
					}

					success = reply.Success

					// Check for success
					if !success {
						if reply.XTerm == -1 {
							// Follower does not contain entry at prevLogIndex
							nextIndex = reply.XLen - 1
						} else {

							// Check if leader contains mismatching term
							// If it does set nextIndex to leader's last occurence of that term
							// If it doesn't then set nextIndex to XIndex (followers first occurence of that term)
							lastIndex := -1
							for i := args.PrevLogIndex - 1; i >= 0; i-- {
								// Skip entries with term greater than XTerm
								if rf.log[i].Term > reply.XTerm {
									continue
								}

								// If XTerm exists this will be the last entry
								if rf.log[i].Term == reply.XTerm {
									lastIndex = i
								}

								break
							}

							if lastIndex == -1 {
								nextIndex = reply.XIndex
							} else {
								nextIndex = lastIndex
							}
						}
					} else {
						// Update follower's next and match index
						rf.nextIndex[server] = postAppendLength
						rf.matchIndex[server] = postAppendLength - 1

						// Acknowledge successful replication
						ackCh <- struct{}{}
					}
				}

			}(postAppendLength, ackCh)
		}
	}

	replicationCount := 1

	// Spin off goroutine to wait for acknowledgements from followers (maybe have a timeout)
	go func(ackCh chan struct{}, logLength int) {
		//
		for range ackCh { // TODO: Maybe we want to exit this loop if we are no longer leader - think of goroutine leakage
			replicationCount++

			if replicationCount > (len(rf.peers) / 2) {
				// Replicated on majority - apply/commit entries
				applyMsg := raftapi.ApplyMsg{
					Command:      command,
					CommandValid: true,
					CommandIndex: logLength - 1,
				}
				rf.applyCh <- applyMsg

				break
			}
		}
	}(ackCh, postAppendLength)
}

func (rf *Raft) startHeartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	// Send intial heartbeats
	rf.sendHeartbeats(term)

	ticker := time.NewTicker(time.Millisecond * 100)

	// Send periodic heartbeats
	for !rf.killed() {

		<-ticker.C

		rf.mu.RLock()
		term = rf.currentTerm
		isLeader := rf.state == LEADER
		rf.mu.RUnlock()

		if isLeader {
			rf.sendHeartbeats(term)
		} else {
			// No longer leader - stop sending heartbeats
			log.Info().Msgf("[%d] NOT LEADER - STOP SENDING HEARTBEATS", rf.me)

			ticker.Stop()
			return
		}
	}
}

func (rf *Raft) sendHeartbeats(term int) {
	for i := range rf.peers {
		if i != rf.me {
			args := &AppendEntriesArgs{
				Term:    term,
				Entries: []Log{},
			}
			reply := &AppendEntriesReply{}

			go func(server int) {
				ok := rf.sendAppendEntries(server, args, reply)
				if !ok {
					log.Warn().Msgf("rpc append entries heartbeat failed for server: %d", server)
				}
			}(i)
		}
	}
}

// Returns a rand timeout in the range of 1000 - 1500 milliseconds
func randTimeout() time.Duration {
	ms := rand.Intn(501) + 1000
	return time.Duration(ms) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.heartbeatChan = make(chan struct{})
	rf.log = []Log{{Term: 0}}
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.mu = &sync.RWMutex{}
	rf.cond = sync.NewCond(rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.logCommitWorker()

	return rf
}
