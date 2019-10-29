package raft

func (rf *Raft) recviedAppendEntries(command interface{}) {

	rf.msgChan <- RecivedMsg
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%d -- %d :::: logs : %v, entries : %v, lastPrevIndex %d, commited %d , apply : %d", args.LeaderID, rf.me, rf.logs, args.Entries, args.PrevLogIndex, rf.commitIndex, rf.lastApplied)
	rf.mu.Unlock()
	go rf.recviedAppendEntries(args.Entries)
	defer rf.updateAppliedLock()

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	//logsLen := len(rf.logs)
	rf.mu.Unlock()

	reply.Success = false
	rf.updateTermLock(args.Term)

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		DPrintf("case1")
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	//case2 => pre log does not match
	rf.mu.Lock()
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("case2")
		rf.mu.Unlock()
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
	//DPrintf(" args.LeaderCommit %d, other  %d", args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	rf.mu.Lock()

	if rf.commitIndex < nextCommitIndex {
		rf.commitIndex = nextCommitIndex
	}
	rf.mu.Unlock()
	//heartBeat

	reply.Success = true
	if len(args.Entries) == 0 {
		return
	}
	rf.mu.Lock()

	if len(rf.logs) > args.PrevLogIndex+1 && rf.logs[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	} else if len(rf.logs) > args.PrevLogIndex+1 && rf.logs[args.PrevLogIndex+1].Term == args.Entries[0].Term {
		rf.mu.Unlock()
		reply.EffectiveAppend = args.PrevLogIndex + 2
		return
	}
	rf.mu.Unlock()

	//case4 => append the new entry
	rf.mu.Lock()
	rf.logs = append(rf.logs, make([]LogEntry, len(args.Entries))...)

	for i, cnt := args.PrevLogIndex+1, 0; cnt < len(args.Entries); {
		rf.logs[i] = args.Entries[cnt]
		cnt++
		i++
		//rf.logs = append(rf.logs, args.Entries[i])
	}
	reply.EffectiveAppend = len(rf.logs)
	DPrintf("reply.EffectiveAppend %d", reply.EffectiveAppend)
	// for i, cnt := args.PrevLogIndex+1, 0; cnt < len(args.Entries); {
	// 	rf.logs[i] = args.Entries[cnt]
	// 	cnt++
	// 	i++
	// }
	rf.mu.Unlock()
	//case5 => update commitIndex

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	for !rf.peers[server].Call("Raft.AppendEntries", args, reply) {

	}
	ok := reply.Success
	//TODO: fix it
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	if rf.updateTermLock(reply.Term) {
		return false
	}
	rf.mu.Lock()
	if !reply.Success {
		//DPrintf("change")
		rf.nextIndex[server]--
	} else if len(args.Entries) != 0 {
		rf.nextIndex[server] = reply.EffectiveAppend
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		//= len(args.Entries)
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) logDuplicate(command interface{}) int {

	defer rf.updateAppliedLock()
	appendEntryTemplate := AppendEntriesArgs{
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	if command == nil {
		appendEntryTemplate.Entries = nil
	}

	rf.mu.Lock()
	DPrintf("\nLeader[%d] Term[%d] Commited[%d] Apply[%d] logs : :  %v  nextIndex: %v matchIndex : %v\n\n", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs, rf.nextIndex, rf.matchIndex)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(idx int) {
				args := appendEntryTemplate
				reply := AppendEntriesReply{}
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[idx] - 1
				//DPrintf("PrevLogIndex : %d, len(logs) : %d", args.PrevLogIndex, len(rf.logs))
				args.PrevLogTerm = rf.logs[rf.nextIndex[idx]-1].Term

				if rf.nextIndex[idx] <= len(rf.logs) {
					args.Entries = rf.logs[rf.nextIndex[idx]:]
				}

				rf.mu.Unlock()
				for !rf.sendAppendEntries(idx, &args, &reply) {
					rf.mu.Lock()
					isLeader := rf.role == Leader
					if !isLeader {
						rf.mu.Unlock()
						break
					}
					args = appendEntryTemplate
					reply = AppendEntriesReply{}
					args.Term = rf.currentTerm
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[idx] - 1
					//DPrintf("PrevLogIndex : %d, len(logs) : %d", args.PrevLogIndex, len(rf.logs))
					args.PrevLogTerm = rf.logs[rf.nextIndex[idx]-1].Term

					if rf.nextIndex[idx] <= len(rf.logs) { //18:34
						args.Entries = rf.logs[rf.nextIndex[idx]:]
						//DPrintf("Server %d entries %v", idx, args.Entries)
					}
					rf.mu.Unlock()
				}
				go func() {
					upper := len(rf.peers) / 2
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.role != Leader {
						return
					}
					for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
						cnt := 0
						for j := 0; j < len(rf.peers); j++ {
							if j != rf.me && i <= rf.matchIndex[j] {
								cnt++
							}
						}
						//DPrintf("cnt : %d, i % d", cnt, i)
						if cnt >= upper {
							if rf.logs[i].Term == rf.currentTerm {
								rf.commitIndex = i
								//DPrintf("commit index becomes %d", i)
								go rf.updateAppliedLock()
							}
						}
					}
				}()
			}(i)
		}
	}

	return 1
}
