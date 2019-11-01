package raft

import "time"

func (rf *Raft) recviedAppendEntries(leader int) {
	//DPrintf("[Append] : Sever[%d] recived Msg From [%d]", rf.me, leader)
	rf.msgChan <- RecivedMsg
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.updateTermLock(args.Term)
	defer rf.updateAppliedLock()

	if rf.role == None {
		return
	}
	reply.Term = rf.currentTerm
	//logsLen := len(rf.logs)
	reply.LastConflictIndex = -1
	reply.LastConflictTerm = -1
	reply.Success = false

	//DPrintf("[DEBUG] l %d t %d , f %d t %d", args.LeaderID, args.Term, rf.me, rf.currentTerm)
	if args.Term < rf.currentTerm {
		rf.persist()
		return
	}

	notFollower := rf.role != Follower
	//DPrintf("[DEBUG] Server %d from %d to Follower {AppendEntries}", rf.me, rf.role)
	rf.role = Follower
	DPrintf("[Commit] Follower %d Term %d Leader[%d] Term[%d]", rf.me, rf.commitIndex, args.LeaderID, args.LeaderCommit)

	if notFollower {
		rf.recviedAppendEntries(args.LeaderID)
	} else {
		rf.recviedAppendEntries(args.LeaderID) //12:00
	}
	rf.mu.Lock()
	//case2 => pre log does not match
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		//DPrintf("case2")

		if len(rf.logs) <= args.PrevLogIndex {
			reply.LastConflictTerm = rf.logs[len(rf.logs)-1].Term
			reply.LastConflictIndex = len(rf.logs)
			rf.mu.Unlock()
			rf.persist()
			return
		}
		var i int
		for i = args.PrevLogIndex - 1; i > 0; i-- {
			if rf.logs[i].Term != rf.logs[args.PrevLogIndex].Term {
				break
			}
		}
		reply.LastConflictTerm = rf.logs[args.PrevLogIndex].Term
		reply.LastConflictIndex = i + 1 //this optimized maybe wrong
		rf.mu.Unlock()
		rf.persist()
		return
	}
	rf.mu.Unlock()

	//case3 => current log does not match
	var nextCommitIndex int
	if args.LeaderCommit < args.PrevLogIndex+len(args.Entries) {
		nextCommitIndex = args.LeaderCommit
	} else {
		nextCommitIndex = args.PrevLogIndex + len(args.Entries)
	}

	rf.mu.Lock()
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = nextCommitIndex
	}
	rf.mu.Unlock()
	//DPrintf("[Logs] Server [%d] %d prevTerm %d prevIndex %d %v entry %v LeaderCommit %d", rf.me, rf.commitIndex, args.PrevLogTerm, args.PrevLogIndex, rf.logs, args.Entries, args.LeaderCommit)

	//heartBeat
	reply.Success = true
	if len(args.Entries) == 0 {
		//DPrintf("[HeratBeat] Server[%d] : committed %d len(logs) %v, entries %v", rf.me, rf.commitIndex, len(rf.logs), args.Entries)
		rf.persist()
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs) > args.PrevLogIndex+1 && rf.logs[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	} else if len(rf.logs) > args.PrevLogIndex+1 && rf.logs[args.PrevLogIndex+1].Term == args.Entries[0].Term {
		if len(rf.logs) > args.PrevLogIndex+1+len(args.Entries) {
			for i := 0; i < len(args.Entries); i++ {
				rf.logs[i+args.PrevLogIndex+1] = args.Entries[i]
			}
			reply.EffectiveAppend = args.PrevLogIndex + 2
			rf.mu.Unlock()
			rf.persist()
			rf.mu.Lock()
			return
		}
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}

	//case4 => append the new entry
	rf.logs = append(rf.logs, make([]LogEntry, len(args.Entries))...)
	for i, cnt := args.PrevLogIndex+1, 0; cnt < len(args.Entries); {
		rf.logs[i] = args.Entries[cnt]
		cnt++
		i++
	}
	rf.mu.Unlock()
	rf.updateAppliedLock()
	rf.persist()
	rf.mu.Lock()
	reply.EffectiveAppend = len(rf.logs)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := AppendEntriesReply{}
	for !rf.peers[server].Call("Raft.AppendEntries", args, &reply) {
		time.Sleep(10 * time.Millisecond)
	}

	ok := reply.Success
	if rf.updateTermLock(reply.Term) {

		rf.persist()

		return false
	}
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		rf.persist()

		return false
	}

	if args.Term != rf.currentTerm {
		rf.mu.Unlock()
		rf.persist()

		return false
	}

	if !reply.Success {
		//DPrintf("[Commitbefore] Server [%d] : %d", server, rf.nextIndex[server])
		if reply.LastConflictIndex >= rf.nextIndex[server] || reply.LastConflictIndex == -1 {
			rf.nextIndex[server]--
		} else {
			rf.nextIndex[server] = reply.LastConflictIndex
			//DPrintf("[Commit] Server [%d] : %d", server, rf.nextIndex[server])
		}
		if rf.nextIndex[server] <= 0 {
			rf.nextIndex[server] = 1
		}

	} else if len(args.Entries) != 0 {
		if reply.EffectiveAppend <= 0 {
			reply.EffectiveAppend = 1
		}
		rf.nextIndex[server] = reply.EffectiveAppend
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	}
	rf.mu.Unlock()
	rf.persist()
	return ok
}

func (rf *Raft) logDuplicate() int {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(idx int) {
				for ok := true; ok; {
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}

					appendEntryTemplate := AppendEntriesArgs{
						LeaderID: rf.me,
					}
					args := appendEntryTemplate

					logs := rf.logs
					args.PrevLogIndex = rf.nextIndex[idx] - 1
					args.Term = rf.currentTerm
					args.LeaderCommit = rf.commitIndex
					args.PrevLogTerm = logs[rf.nextIndex[idx]-1].Term

					if rf.nextIndex[idx] <= len(logs) {
						args.Entries = logs[rf.nextIndex[idx]:]
					}
					rf.mu.Unlock()
					ok = !rf.sendAppendEntries(idx, &args)
				}
				upper := len(rf.peers) / 2
				rf.mu.Lock()
				for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
					cnt := 0
					if rf.logs[i].Term < rf.currentTerm {
						break
					}
					for j := 0; j < len(rf.peers); j++ {
						if j != rf.me && i <= rf.matchIndex[j] {
							cnt++
						}
					}
					if cnt >= upper {
						if rf.logs[i].Term == rf.currentTerm {
							rf.commitIndex = i
							break
						}
					}
				}
				rf.mu.Unlock()
				rf.updateAppliedLock()

			}(i)
		}
	}
	return 1
}
