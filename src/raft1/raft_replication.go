package raft

import (
	"math"

	"6.5840/raftapi"
	"github.com/rs/zerolog/log"
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		// Revert to follower
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1

		// Perform log consistency check
		// Check if entry exists at prevLogIndex
		if args.PrevLogIndex >= len(rf.log) {
			// Entry doesn't exist
			reply.XLen = len(rf.log)
			reply.XTerm = -1

			return

		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Mismatching log terms
			reply.XTerm = rf.log[args.PrevLogIndex].Term

			// Get first index of that term in log
			firstIndex := args.PrevLogIndex
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
					break
				}
				firstIndex = i
			}

			reply.XIndex = firstIndex

			rf.log = rf.log[:args.PrevLogIndex]

			rf.persist()

			return
		} else {
			// Matching log terms

			// last index that has been verified by the leader
			lastVerifiedIndex := len(rf.log) - 1

			if len(args.Entries) == 0 {
				// Notify heartbeat chan to reset election timer
				rf.mu.Unlock()
				rf.heartbeatCh <- struct{}{}
				rf.mu.Lock()

				// heartbeat's prevLogIndex is the last entry the leader has verified
				lastVerifiedIndex = args.PrevLogIndex

			} else {
				if len(rf.log) == args.PrevLogIndex+1 {
					// Append leader's new entries
					rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
				} else {
					// Follower contains entries beyond those being appended
					nextIndex := args.PrevLogIndex + 1

					// Write new entries without removing subsequent entries
					for i := range args.Entries {
						if nextIndex >= len(rf.log) {
							// Beyond existing log length -- append the rest
							rf.log = append(rf.log, args.Entries[i:]...)
							break
						}

						// Overwrite log entry
						rf.log[nextIndex] = args.Entries[i]
						nextIndex++
					}
				}

				rf.persist()
			}

			// Check if logs needs to be committed
			// If commit index has updated follower should commit up to the index of its last verified entry
			// Take min (lastVerifiedIndex, leaderCommit) to prevent committing false entries in log
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(lastVerifiedIndex), float64(args.LeaderCommit)))

				// Signal to commit log entries
				commitIndex := rf.commitIndex // copy before releasing lock

				rf.mu.Unlock()
				rf.commitCh <- commitIndex // Signal commit worker to commit entries
				rf.mu.Lock()
			}
		}

		reply.Success = true
	}
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
	term = rf.currentTerm
	index = len(rf.log)

	// Append to local log
	rf.log = append(rf.log, Log{
		Term:    rf.currentTerm,
		Command: command,
	})

	rf.persist()

	rf.readPersist(rf.persister.ReadRaftState())

	log.Info().Msgf("[%d] Appending entry %v at index %d", rf.me, command, len(rf.log)-1)

	// Send append requests to followers
	go rf.appendEntries()

	return index, term, isLeader
}

func (rf *Raft) appendEntries() {
	rf.mu.Lock()
	// Append to leader's log first
	commandIndex := len(rf.log) - 1

	rf.mu.Unlock()

	// Send latest logs to each server
	for server := range rf.peers {
		if server != rf.me {
			go func(server, commandIndex int) {
				rf.mu.Lock()
				nextIndex := rf.nextIndex[server]
				rf.mu.Unlock()

				success := false

				for !success && !rf.killed() {
					rf.mu.Lock()

					// Prevent next index from being zero on retries
					if nextIndex == 0 {
						nextIndex = 1
					}

					// Exit if no longer leader
					if rf.state != LEADER { // TODO: review if this works or can be improved
						rf.mu.Unlock()
						return
					}

					// Create copy of entries from log slice
					entries := make([]Log, len(rf.log[nextIndex:]))
					copy(entries, rf.log[nextIndex:])

					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIndex: nextIndex - 1,
						PrevLogTerm:  rf.log[nextIndex-1].Term,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}

					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
						//log.Warn().Msgf("rpc append entries failed for [%d]", server)
					}

					rf.mu.Lock()
					// Confirm we are still leader
					if reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						rf.currentTerm = args.Term
						rf.votedFor = -1
						rf.mu.Unlock()

						return
					}
					rf.mu.Unlock()

					success = reply.Success

					// Check for success
					if !success {
						if reply.XTerm == -1 {
							// Follower does not contain entry at prevLogIndex
							//log.Info().Msgf("[%d] Append entries - follower %d does not contain entry", rf.me, server)

							nextIndex = reply.XLen - 1
						} else {
							//log.Info().Msgf("[%d] Append entries - follower %d has mismatching entry", rf.me, server)

							// Check if leader contains mismatching term
							// If it does set nextIndex to leader's last occurence of that term
							// If it doesn't then set nextIndex to XIndex (followers first occurence of that term)
							lastIndex := -1
							for i := args.PrevLogIndex - 1; i >= 0; i-- {
								// Skip entries with term greater than XTerm
								rf.mu.Lock()
								logTerm := rf.log[i].Term
								rf.mu.Unlock()

								if logTerm > reply.XTerm {
									continue
								}

								// If XTerm exists this will be the last entry
								if logTerm == reply.XTerm {
									lastIndex = i
								}

								break
							}

							if lastIndex == -1 {
								// Leader doesn't contain mismatching entry
								nextIndex = reply.XIndex
							} else {
								// Leader contains mismatching entry
								nextIndex = lastIndex
							}
						}
					} else {
						log.Info().Msgf("[%d] Append entries successful to %d", rf.me, server)

						rf.mu.Lock()
						defer rf.mu.Unlock()

						// Update follower's next and match index
						// Guard against old commands being processed
						if rf.nextIndex[server] < commandIndex+1 {
							rf.nextIndex[server] = commandIndex + 1
						}
						if rf.matchIndex[server] < commandIndex {
							rf.matchIndex[server] = commandIndex

							// Check for highest commit index
							for i := range len(rf.peers) {
								if i != rf.me {
									if rf.matchIndex[i] > commandIndex {
										commandIndex = rf.matchIndex[i]
									}
								}
							}

							// Check if commit index has been replicated on a majority of servers
							replications := 1
							commitIndex := commandIndex
							for i := range len(rf.peers) {
								if i != rf.me {
									if rf.matchIndex[i] == commitIndex {
										replications++
									}
								}
							}

							if replications > (len(rf.peers)/2) && commitIndex > rf.commitIndex {
								// Signal to commit worker
								rf.mu.Unlock()
								rf.commitCh <- commitIndex
								rf.mu.Lock()
							}
						}
					}
				}
			}(server, commandIndex)
		}
	}
}

func (rf *Raft) logCommitWorker() {
	for !rf.killed() {
		select {
		case commitIndex := <-rf.commitCh:
			rf.mu.Lock()

			//log.Info().Msgf("[%d] LOG - %v", rf.me, rf.log)

			log.Info().Msgf("[%d] COMMITTING from [%d] to [%d]", rf.me, rf.lastApplied+1, commitIndex)

			// Commit logs
			for i := rf.lastApplied + 1; i <= commitIndex; i++ {
				applyMsg := raftapi.ApplyMsg{
					Command:      rf.log[i].Command,
					CommandValid: true,
					CommandIndex: i,
				}

				//
				//log.Info().Msgf("[%d] COMMITTING cmd(%v) entry at %d", rf.me, applyMsg.Command, i)

				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()

				rf.lastApplied = i
				rf.commitIndex = i
			}

			rf.mu.Unlock()

		case <-rf.ctx.Done():
			return
		}
	}
}
