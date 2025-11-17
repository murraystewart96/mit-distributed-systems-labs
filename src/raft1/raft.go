package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm uint
	votedFor    int
	state       State

	log []Log

	commitIndex uint
	lastApplied uint

	heartbeatChan chan struct{}
}

type Log struct {
	Term    uint
	Command any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	// Your code here (3A).
	return int(rf.currentTerm), rf.state == LEADER
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
	Term             uint
	LeaderID         int
	PreviousLogIndex uint
	Entries          []Log
}

type AppendEntriesReply struct {
	Term    uint
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
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
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint
	CandidateID  int
	LastLogIndex uint
	LastLogTerm  uint
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint
	VoteGranted bool
}

// example RequestVote RPC handler.
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
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= uint(lastLogIndex) {
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
	index := -1
	term := -1
	isLeader := true

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
			//log.Info().Msgf("server %d received heartbeat - restarting election timer", rf.me)
			electionTimer.Reset(randTimeout())

		case <-electionDone:
			electionTimer.Reset(randTimeout())
		}
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
	for i := range rf.peers {
		if i != rf.me {
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
			}(i, voteCh)
		}
	}
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
		select {
		case <-ticker.C:

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
}

func (rf *Raft) sendHeartbeats(term uint) {
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
