package raft

import (
	"time"
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

	defer rf.updateAppliedLock()
	//Your code here (2A, 2B).
	rf.updateTermLock(args.Term)
	go func() {
		rf.msgChan <- RecivedVoteRequest
		DPrintf("send a RecivedVoteRequest msg [---]\n\n")
	}()
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
	currentLastLogIndex = len(rf.logs) //TODO: fix the length corner case
	currentLastLogTerm = rf.logs[len(rf.logs)-1].Term
	votedFor = rf.votedFor
	isLeader = rf.role == Leader
	DPrintf("Sever [%d] recived server [%d]'s voteRequest\n\n", rf.me, args.CandidateID)
	rf.mu.Unlock()

	//case 0 => I'm leader, so you must stop election
	if isLeader {
		DPrintf("case0\n")
		return
	}

	//case 1 => the candidate is not suit to be voted
	if currentTerm > candidateTerm {
		DPrintf("case1\n")
		return
	}

	//case 2 => the candidate's log is not lastest than the follwer
	if currentLastLogTerm > candidateLastLogTerm || (currentLastLogTerm == candidateLastLogTerm && currentLastLogIndex > candidateLastLogIndex) {
		DPrintf("case2\n")
		return
	}

	//case3 => I have voted and is not you
	if votedFor != nil && votedFor != candidateID {
		DPrintf("case3\n")
		return
	}
	DPrintf("Sever [%d] decide to vote [%d]\n\n", rf.me, args.CandidateID)

	//now I will vote you
	rf.mu.Lock()
	rf.votedFor = candidateID
	rf.mu.Unlock()
	reply.VoteCranted = true
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	ok = ok && reply.VoteCranted
	ok = ok && !rf.updateTermLock(reply.Term)
	return ok
}

//a follower try to elect the other servers' vote to be a leader
func (rf *Raft) tryToBeLeader() int {
	//Step 1
	var maxVoteNum, currentVoteNum, currentSuccessNum int
	var templateArgs RequestVoteArgs
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	//rf.timer.Reset(time.Duration(rand.Int())) //TODO: give a random or use sleep
	maxVoteNum = len(rf.peers)

	templateArgs.Term = rf.currentTerm
	templateArgs.CandidateID = rf.me
	//DPrintf("Server [%d]  logs : %v\n\n", rf.me, rf.logs)
	templateArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	templateArgs.LastLogIndex = len(rf.logs)
	rf.mu.Unlock()

	//channel to notify  timeout or be leader
	electionEnd := make(chan int, 1)
	//start timeout
	go func() {
		time.Sleep(randomTimeOut()) //TODO: give a random or use sleep
		DPrintf("here timeOutTimer\n")
		electionEnd <- TimeOut
	}()

	//waitGroup to wait for the most goroutine wake
	//var wg sync.WaitGroup
	voteChan := make(chan bool, 1)
	currentVoteNum = 1
	currentSuccessNum = 1
	//go routine to wait the majority server to voted me
	go func() {
		for {
			ok := <-voteChan
			currentVoteNum++
			if ok {
				currentSuccessNum++
			}
			DPrintf("Server [%d] currentVote : %d, maxVote : %d \n\n", rf.me, currentSuccessNum, maxVoteNum)
			if currentSuccessNum >= (maxVoteNum+1)/2 {

				electionEnd <- BecomeLeader
				return
			}
			if currentVoteNum >= maxVoteNum {
				//when recived all vote but can not be leader
				return
			}
		}
	}()

	// wg.Add(maxVoteNum - 1)
	for i := 0; i < maxVoteNum; i++ {
		if i != rf.me {
			go func(idx int) {
				args := templateArgs
				var reply RequestVoteReply
				ok := rf.sendRequestVote(idx, &args, &reply)
				//DPrintf("recived %d reply", idx)
				var aLeaderComeUp bool
				rf.mu.Lock()
				aLeaderComeUp = rf.role != Candidate
				rf.mu.Unlock()
				//DPrintf("aLeaderComeUp ? %v", aLeaderComeUp)
				if aLeaderComeUp {
					go func() { electionEnd <- BecomeFollower }()
				} else {
					go func() { voteChan <- ok }()
				}
			}(i)
		}
	}
	result := <-electionEnd
	return result
}
