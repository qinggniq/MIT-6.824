package raft

import (
	"sync"
)

//end reson
const (
	BecomeLeader       = 0
	BecomeFollower     = 1
	TimeOut            = 2
	RecivedMsg         = 3
	RecivedVoteRequest = 4
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Term = servers.CurrentTerm
	VoteCranted bool //false if candidate.Term < servers.CurrentTerm
}

//
// RequestVote
// caller ; candidate
// reciver : follower
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	//defer rf.updateAppliedLock()
	//Your code here (2A, 2B).
	rf.updateTermLock(args.Term)
	reply.VoteCranted = false
	var votedFor interface{}
	var isLeader bool
	var candidateID, currentTerm, candidateTerm, currentLastLogIndex, candidateLastLogIndex, currentLastLogTerm, candidateLastLogTerm int

	candidateID = args.CandidateID
	candidateTerm = args.Term
	candidateLastLogIndex = args.LastLogIndex
	candidateLastLogTerm = args.LastLogTerm

	rf.mu.Lock()

	reply.Term = rf.currentTerm
	currentTerm = rf.currentTerm
	currentLastLogIndex = len(rf.logs) - 1 //TODO: fix the length corner case
	currentLastLogTerm = rf.logs[len(rf.logs)-1].Term
	votedFor = rf.votedFor
	isLeader = rf.role == Leader

	//DPrintf("[DEBUG] c %d  %d-- f %d %d %v", candidateID, candidateLastLogTerm, rf.me, currentLastLogTerm, rf.logs)

	//case 0 => I'm leader, so you must stop election
	if isLeader {
		rf.mu.Unlock()
		return
	}

	//case 1 => the candidate is not suit to be voted
	if currentTerm > candidateTerm {
		rf.mu.Unlock()
		return
	}

	//case 2 => the candidate's log is not lastest than the follwer
	if currentLastLogTerm > candidateLastLogTerm || (currentLastLogTerm == candidateLastLogTerm && currentLastLogIndex > candidateLastLogIndex) {
		rf.mu.Unlock()
		return
	}

	//case3 => I have voted and is not you
	if votedFor != nil && votedFor != candidateID {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//now I will vote you
	var notFollower bool
	rf.mu.Lock()
	rf.votedFor = candidateID
	if rf.role != Follower {
		notFollower = true
	}
	rf.role = Follower
	reply.VoteCranted = true
	rf.mu.Unlock()
	if notFollower {
		rf.msgChan <- RecivedVoteRequest
	}

	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if rf.updateTermLock(reply.Term) {
		return false
	}
	rf.mu.Lock()
	if rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	ok = ok && reply.VoteCranted

	return ok
}

//a follower try to elect the other servers' vote to be a leader
func (rf *Raft) tryToBeLeader() {
	//Step 1
	DPrintf("[DEBUG] : Sever %d, Status %d", rf.me, rf.role)
	var maxVoteNum, currentSuccessNum int

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	maxVoteNum = len(rf.peers)
	rf.mu.Unlock()

	currentSuccessNum = 1
	var mutex sync.Mutex
	for i := 0; i < maxVoteNum; i++ {
		if i != rf.me {
			go func(idx int) {
				var templateArgs RequestVoteArgs
				rf.mu.Lock()
				aLeaderComeUp := rf.role == Follower || rf.role == Leader
				rf.mu.Unlock()
				if aLeaderComeUp {
					return
				}
				rf.mu.Lock()
				templateArgs.Term = rf.currentTerm
				templateArgs.CandidateID = rf.me
				templateArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				templateArgs.LastLogIndex = len(rf.logs) - 1
				rf.mu.Unlock()

				args := templateArgs
				var reply RequestVoteReply
				ok := rf.sendRequestVote(idx, &args, &reply)

				rf.mu.Lock()
				aLeaderComeUp = rf.role == Follower || rf.role == Leader
				rf.mu.Unlock()
				if aLeaderComeUp {
					return
				} else {
					if ok {
						mutex.Lock()
						currentSuccessNum++
						mutex.Unlock()
						if currentSuccessNum >= maxVoteNum/2+1 {
							rf.mu.Lock()
							rf.role = Leader
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0
							}
							rf.mu.Unlock()
							rf.logDuplicate()
							rf.msgChan <- BecomeLeader
							return
						}
					}
				}
			}(i)
		}
	}

}
