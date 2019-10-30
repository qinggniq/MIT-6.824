package raft

func (rf *Raft) recviedAppendEntries(command interface{}) {

	rf.msgChan <- RecivedMsg
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.updateTermLock(args.Term)
	defer rf.updateAppliedLock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//logsLen := len(rf.logs)

	reply.Success = false

	//DPrintf("[DEBUG] l %d t %d , f %d t %d", args.LeaderID, args.Term, rf.me, rf.currentTerm)
	if args.Term < rf.currentTerm {
		return
	}

	go rf.recviedAppendEntries(args.Entries)
	//case2 => pre log does not match
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("case2")

		return
	}

	//case3 => current log does not match
	var nextCommitIndex int
	if args.LeaderCommit < args.PrevLogIndex+len(args.Entries) {
		nextCommitIndex = args.LeaderCommit
	} else {
		nextCommitIndex = args.PrevLogIndex + len(args.Entries)
	}
	//DPrintf(" args.LeaderCommit %d, other  %d", args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = nextCommitIndex
	}

	//heartBeat

	reply.Success = true
	if len(args.Entries) == 0 {
		//rf.updateAppliedLock()
		return
	}

	if len(rf.logs) > args.PrevLogIndex+1 && rf.logs[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	} else if len(rf.logs) > args.PrevLogIndex+1 && rf.logs[args.PrevLogIndex+1].Term == args.Entries[0].Term {
		if len(rf.logs) > args.PrevLogIndex+1+len(args.Entries) {

			//rf.updateAppliedLock()
			reply.EffectiveAppend = args.PrevLogIndex + 2
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
		//rf.logs = append(rf.logs, args.Entries[i])
	}
	reply.EffectiveAppend = len(rf.logs)
	//DPrintf("reply.EffectiveAppend %d", reply.EffectiveAppend)

	//rf.updateAppliedLock()
	//case5 => update commitIndex

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {

	reply := AppendEntriesReply{}
	for !rf.peers[server].Call("Raft.AppendEntries", args, &reply) {

	}

	ok := reply.Success
	//TODO: fix it
	if rf.updateTermLock(reply.Term) {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return false
	}

	if args.Term != rf.currentTerm {
		return false
	}

	if !reply.Success {
		rf.nextIndex[server]--
	} else if len(args.Entries) != 0 {
		if reply.EffectiveAppend <= 0 {
			reply.EffectiveAppend = 1
		}
		rf.nextIndex[server] = reply.EffectiveAppend

		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	}
	return ok
}

func (rf *Raft) logDuplicate() int {

	rf.updateAppliedLock()

	//rf.mu.Lock()
	//DPrintf("\nLeader[%d] Term[%d] Commited[%d] Apply[%d] logs : :  %v  nextIndex: %v matchIndex : %v\n\n", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs, rf.nextIndex, rf.matchIndex)
	//rf.mu.Unlock()

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
					//reply := AppendEntriesReply{}

					args.Term = rf.currentTerm
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[idx] - 1
					//DPrintf("PrevLogIndex : %d, len(logs) : %d", args.PrevLogIndex, len(rf.logs))
					args.PrevLogTerm = rf.logs[rf.nextIndex[idx]-1].Term

					if rf.nextIndex[idx] <= len(rf.logs) {
						args.Entries = rf.logs[rf.nextIndex[idx]:]
					}
					rf.mu.Unlock()
					ok = !rf.sendAppendEntries(idx, &args)

					//time.Sleep(time.Millisecond * 100)
				}
				upper := len(rf.peers) / 2
				rf.mu.Lock()
				for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
					cnt := 0
					for j := 0; j < len(rf.peers); j++ {
						if j != rf.me && i <= rf.matchIndex[j] {
							cnt++
						}
					}
					if cnt >= upper {
						if rf.logs[i].Term == rf.currentTerm {
							rf.commitIndex = i
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
