package raft

func (rf *Raft) recviedAppendEntries() {
	rf.msgChan <- RecivedMsg
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	go rf.recviedAppendEntries()
	defer rf.updateTermLock(args.Term)
	defer rf.updateAppliedLock()

	reply.Term = rf.currentTerm
	reply.Success = false

	//case1 => the sender is not a leader
	if args.Term < rf.currentTerm {
		return
	}

	//case2 => pre log does not match
	logsLen := len(rf.logs)
	//TODO: handler the corner case
	if logsLen < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	//case3 => current log does not match
	//heartBeat
	reply.Success = true
	if args.Entries == nil {
		return
	}
	if logsLen > args.PrevLogIndex && rf.logs[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.mu.Lock()
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.mu.Unlock()
	}

	//case4 => append the new entry
	rf.mu.Lock()
	rf.logs = append(rf.logs, make([]LogEntry, len(args.Entries))...)
	for i, cnt := args.PrevLogIndex+1, 0; cnt < len(args.Entries); {
		rf.logs[i] = args.Entries[cnt]
		cnt++
		i++
	}
	rf.mu.Unlock()

	//case5 => update commitIndex
	if rf.commitIndex < args.LeaderCommit {
		if args.LeaderCommit < args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	ok = ok && reply.Success
	rf.mu.Lock()
	if !reply.Success {
		rf.nextIndex[server]--
	} else {
		rf.matchIndex[server] = rf.nextIndex[server]
		rf.nextIndex[server]++
	}
	rf.mu.Unlock()
	ok = ok && !rf.updateTermLock(reply.Term)
	return ok
}

func (rf *Raft) logDuplicate(applyMsg *ApplyMsg) int {

	appendEntryTemplate := AppendEntriesArgs{
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	//applyMsg == nil means is a heartbeats message
	if applyMsg == nil {
		appendEntryTemplate.Entries = nil
	}
	rf.mu.Lock()
	
	rf.mu.Lock()
	totalPeers := len(rf.peers)
	currentSuccessNum := 1
	currentSendNum := 1
	logChan = make(chan bool, 1)
	dupEndChan = make(chan bool, 1)
	go func() {
		for {
			ok := <- logChan
			currentSuccessNum++
			if ok {
				currentSuccessNum++
			}
			if currentSuccessNum >= (totalPeers + 1) / 2{
				dupEndChan <- true
			}
		}
	}()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(idx int) {
				args := appendEntryTemplate
				reply := AppendEntriesReply{}
				args.PrevLogIndex = rf.nextIndex[idx] - 1
				args.PrevLogTerm = rf.logs[rf.nextIndex[idx]-1].Term
				if applyMsg == nil {
					args.Entries = nil
				} else {
					args.Entries = make([]LogEntry, 0)
					args.Entries = append(args.Entries, rf.logs[rf.nextIndex[idx]])
				}
				for ok := rf.sendAppendEntries(idx, &args, &reply); !ok {
				}
				go func() {
					logChan <- true	
				}()

			}(i)
		}
	}
	<- dupEndChan
	return 1
}
