package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	mu        *sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	ctx       context.Context
	cancel    context.CancelFunc

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	heartbeatCh chan struct{}
	commitCh    chan int
	applyCh     chan raftapi.ApplyMsg
	cond        *sync.Cond

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatal().Err(err).Msg("failed to encode current term")
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatal().Err(err).Msg("failed to encode voted for")
	}
	if err := e.Encode(rf.log); err != nil {
		log.Fatal().Err(err).Msg("failed to encode log")
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var tempLog []Log

	if err := d.Decode(&currentTerm); err != nil {
		log.Fatal().Err(err).Msg("failed to decode current term")
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Fatal().Err(err).Msg("failed to decode voted for")
	}
	if err := d.Decode(&tempLog); err != nil {
		log.Fatal().Err(err).Msg("failed to decode log")
	}

	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
	rf.log = tempLog
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
		rf.persist()
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
			rf.persist()
			// maybe reset timer
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
	rf.cancel()
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
			rf.mu.Lock()

			if rf.state == FOLLOWER {
				go rf.startElection(electionDone)
			}

			rf.mu.Unlock()

		case <-rf.heartbeatCh:
			// Leader is alive -> reset election timeout (still follower)
			electionTimer.Reset(randTimeout())

		case <-electionDone:
			electionTimer.Reset(randTimeout())

		case <-rf.ctx.Done():
			return
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
		rf.persist()

		log.Info().Msgf("[%d] starting election for term %d", rf.me, rf.currentTerm)

		rf.mu.Unlock()

		// Request votes for this election
		votes := 1
		voteCh := make(chan struct{})

		reqVotesCtx, cancelReqVotes := context.WithTimeout(rf.ctx, timeout)
		rf.requestVotes(reqVotesCtx, voteCh)

		// Wait for votes or timeout
		electionOver := false
		for !electionOver {
			select {
			case <-timer.C:
				electionOver = true

				rf.mu.Lock()
				rf.votedFor = -1
				isCandidate = rf.state == CANDIDATE
				rf.persist()

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

					// Init followers nextIndices
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = -1
					}

					// Start sending heartbeats
					go rf.startHeartbeat()

					electionOver = true
					rf.votedFor = -1
					rf.persist()

				}

				rf.mu.Unlock()

			case <-rf.ctx.Done():
				cancelReqVotes()
				return
			}
		}

		// Signal to remaining voters election is over
		cancelReqVotes()

		if electionOver && !isCandidate {
			done <- struct{}{}
		}
	}
}

func (rf *Raft) requestVotes(ctx context.Context, voteCh chan struct{}) {
	// Request vote from other servers
	for server := range rf.peers {
		if server != rf.me {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}

			// Send out vote requests
			go func(server int, voteCh chan struct{}) {
				ok := rf.sendRequestVote(server, args, reply)
				if !ok {
					log.Warn().Msgf("[%d] rpc request vote failed to server: %d", rf.me, server)
				}

				if reply.VoteGranted {
					select {
					case <-ctx.Done():
						return
					case voteCh <- struct{}{}:
						return
					}
				}

				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
				}

				rf.mu.Unlock()
			}(server, voteCh)
		}
	}
}

func (rf *Raft) startHeartbeat() {
	// Send intial heartbeats
	rf.sendHeartbeats()

	ticker := time.NewTicker(time.Millisecond * 100)

	// Send periodic heartbeats
	for !rf.killed() {
		select {
		case <-ticker.C:
			rf.mu.Lock()
			isLeader := rf.state == LEADER
			rf.mu.Unlock()

			if isLeader {
				rf.sendHeartbeats()
			} else {
				// No longer leader - stop sending heartbeats
				log.Info().Msgf("[%d] NOT LEADER - STOP SENDING HEARTBEATS", rf.me)

				ticker.Stop()
				return
			}

		case <-rf.ctx.Done():
			return
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			term := rf.currentTerm
			leaderCommit := rf.commitIndex
			prevIndex := rf.nextIndex[i] - 1
			prevTerm := rf.log[rf.nextIndex[i]-1].Term
			rf.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:         term,
				Entries:      []Log{},
				LeaderCommit: leaderCommit,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
			}
			reply := &AppendEntriesReply{}

			//log.Info().Msgf("[%d] sending heartbeats to %d", rf.me, i)

			go func(server int) {
				ok := rf.sendAppendEntries(server, args, reply)
				if !ok {
					//log.Warn().Msgf("rpc append entries heartbeat failed for server: %d", server)
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
	rf.heartbeatCh = make(chan struct{})
	rf.commitCh = make(chan int)
	rf.log = []Log{{Term: 0}}
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.mu = &sync.Mutex{}
	rf.cond = sync.NewCond(rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.ctx, rf.cancel = context.WithCancel(context.Background())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.logCommitWorker()

	return rf
}
