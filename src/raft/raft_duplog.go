package raft

func (rf *Raft) recviedAppendEntries() {
	rf.msgChan <- RecivedMsg
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	go rf.recviedAppendEntries()

	defer rf.updateAppliedLock()
	rf.updateTermLock(args.Term)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	logsLen := len(rf.logs)
	rf.mu.Unlock()

	reply.Success = false

	//case1 => the sender is not a leader
	if args.Term < rf.currentTerm {
		//DPrintf("case 1  => the sender is not a leader server[%d]'s term %d, leader's Term %d", rf.me, rf.currentTerm, args.Term)
		return
	}

	//case2 => pre log does not match

	//TODO: handler the corner case
	if logsLen < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("/case2 => pre log does not match")
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
	DPrintf("case4 append new entry")
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

	for !rf.peers[server].Call("Raft.AppendEntries", args, reply) {

	}
	ok := reply.Success
	if rf.updateTermLock(reply.Term) {
		DPrintf("Sever[%d] I am not a leader now", rf.me)
		return false
	}
	rf.mu.Lock()
	if !reply.Success {
		rf.nextIndex[server]--
	} else if args.Entries != nil {
		rf.matchIndex[server] = rf.nextIndex[server]
		rf.nextIndex[server]++
	}
	rf.mu.Unlock()

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
	var logsLen int
	if applyMsg != nil {
		rf.mu.Lock()
		logsLen = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: applyMsg})
		rf.mu.Lock()
	}

	totalPeers := len(rf.peers)
	currentSuccessNum := 1
	currentSendNum := 1
	logChan := make(chan bool, 1)
	dupEndChan := make(chan bool, 1)
	go func() {
		for {
			ok := <-logChan
			currentSendNum++
			if ok {
				currentSuccessNum++
			}
			if currentSendNum >= (totalPeers+1)/2 {
				dupEndChan <- true
			}
		}
	}()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(idx int) {
				args := appendEntryTemplate
				reply := AppendEntriesReply{}

				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.PrevLogIndex = rf.nextIndex[idx] - 1
				args.PrevLogTerm = rf.logs[rf.nextIndex[idx]-1].Term

				if applyMsg == nil {
					args.Entries = nil
				} else {

					//TODO: maybe exist corner case
					if rf.nextIndex[idx] <= logsLen {
						args.Entries = rf.logs[rf.nextIndex[idx] : logsLen+1]
					}

				}
				rf.mu.Unlock()
				for rf.sendAppendEntries(idx, &args, &reply) {
				}
				go func() {
					logChan <- true
				}()
			}(i)
		}
	}
	<-dupEndChan
	rf.mu.Lock()
	if rf.role == Leader {
		rf.commitIndex = logsLen
	}
	rf.mu.Unlock()
	return 1
}
