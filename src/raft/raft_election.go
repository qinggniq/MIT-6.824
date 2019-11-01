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
	End                = 5
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
	isALeader := rf.role == Leader

	if rf.updateTermLock(args.Term) && isALeader {
		//DPrintf("[DEBUG] Server %d from %d to Follower  {requestVote : Term higher}", rf.me, Leader)
	}
	reply.VoteCranted = false
	var votedFor interface{}
	//var isLeader bool
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
	isFollower := rf.role == Follower
	rf.mu.Unlock()
	//case 0 => I'm leader, so you must stop election
	if !isFollower {
		DPrintf("[DEBUG] Case0 I [%d] is Candidate than %d", rf.me, args.CandidateID)
		return
	}

	//case 1 => the candidate is not suit to be voted
	if currentTerm > candidateTerm {
		DPrintf("[DEBUG] Case1 Follower %d > Candidate %d ", rf.me, args.CandidateID)
		return
	}

	//case 2 => the candidate's log is not lastest than the follwer
	if currentLastLogTerm > candidateLastLogTerm || (currentLastLogTerm == candidateLastLogTerm && currentLastLogIndex > candidateLastLogIndex) {
		DPrintf("[DEBUG] Case2 don't my[%d] newer than can[%d]", rf.me, args.CandidateID)
		return
	}
	rf.mu.Lock()
	//case3 => I have voted and is not you
	if votedFor != nil && votedFor != candidateID {
		rf.mu.Unlock()
		return
	}

	//now I will vote you

	var notFollower bool
	rf.votedFor = candidateID
	if rf.role != Follower {
		notFollower = true
	}
	DPrintf("[Vote] Server[%d] vote to Can[%d]", rf.me, args.CandidateID)
	rf.role = Follower
	reply.VoteCranted = true
	rf.mu.Unlock()
	rf.persist()
	if notFollower {
		rf.msgChan <- RecivedVoteRequest
	} else {
		rf.msgChan <- RecivedVoteRequest
	}

	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if rf.currentTerm != args.Term {
		rf.mu.Unlock()
		rf.persist()
		return false
	}
	rf.mu.Unlock()

	if rf.updateTermLock(reply.Term) {
		DPrintf("[DEBUG] Candidate %d from %d to follower", rf.me, Candidate)
		rf.persist()
		return false
	}

	return ok && reply.VoteCranted
}

//a follower try to elect the other servers' vote to be a leader
func (rf *Raft) tryToBeLeader() {
	//Step 1
	var maxVoteNum, currentSuccessNum int
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	maxVoteNum = len(rf.peers)
	rf.mu.Unlock()
	rf.persist()

	currentSuccessNum = 1
	var mutex sync.Mutex
	for i := 0; i < maxVoteNum; i++ {
		if i != rf.me {
			go func(idx int) {
				var templateArgs RequestVoteArgs
				rf.mu.Lock()
				aLeaderComeUp := rf.role == Follower || rf.role == Leader

				if aLeaderComeUp {
					rf.mu.Unlock()
					return
				}
				templateArgs.Term = rf.currentTerm
				templateArgs.CandidateID = rf.me
				templateArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				templateArgs.LastLogIndex = len(rf.logs) - 1
				rf.mu.Unlock()

				args := templateArgs
				var reply RequestVoteReply
				ok := rf.sendRequestVote(idx, &args, &reply)

				rf.mu.Lock()
				aLeaderComeUp = rf.role == Follower || rf.role == Leader || rf.role == None
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
							go rf.logDuplicate()
							rf.msgChan <- BecomeLeader
							return
						}
					}
				}
			}(i)
		}
	}

}
