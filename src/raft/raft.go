package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	labrpc "lab/labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//election timeout
const electionTimeOut = 5000

//show the raft status
const (
	Leader    = 0 //leader = 0
	Candidate = 1 //cnadidate = 1
	Follower  = 2 //follower = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// LogEntry : the entry of log
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// AppendEntries : Append entry for all servers to store
//
type AppendEntries struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry //TODO: change type
	LeaderCommit int
}

//AppendEntriesArgs is the AppendEntries
type AppendEntriesArgs AppendEntries

//
//AppendEntriesReply reply
//
type AppendEntriesReply struct {
	Term            int
	Success         bool
	EffectiveAppend int
}

//
// Raft :  A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//Persisitent state on all servers
	currentTerm int
	votedFor    interface{} //TODO change the type
	logs        []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//leader candidate or follower 0 : leaeder, 1 : candidate 2:follower
	role int

	//vote count
	voteCnt int
	//msgChan if appendLogAppend, send msg to msgChan
	msgChan chan int
	//timer wait timeout
	timeOutTimer *time.Timer

	//apply msg chan
	applyChan chan ApplyMsg
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//update the term
func (rf *Raft) updateTermLock(newTerm int) bool {
	//if I am outdated, I must be a follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < newTerm {
		rf.currentTerm = newTerm
		rf.role = Follower
		rf.votedFor = nil
		go func() {
			rf.msgChan <- BecomeFollower
		}()
		return true
	}
	return false
}

//update applied when response the the rpc

func (rf *Raft) updateAppliedLock() bool {
	rf.mu.Lock()

	if rf.commitIndex > rf.lastApplied {

		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		go func(applyMsg ApplyMsg) {
			rf.mu.Lock()
			DPrintf("Leader[%d] apply %v", rf.me, applyMsg)
			rf.mu.Unlock()
			rf.applyChan <- applyMsg
		}(applyMsg)
		return true

	}
	rf.mu.Unlock()
	return false
}

//
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
//

//send AppendEntries

//
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
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == Leader

	if isLeader {
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
		term = rf.currentTerm
		DPrintf("Command {%v}, rf.logs %v", command, rf.logs)
		go rf.logDuplicate(command)
	} else {
		term = rf.currentTerm
	}
	return index, term, isLeader

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func randomTimeOut(isLeader bool) time.Duration {
	var div int
	if isLeader {
		div = 2
	} else {
		div = 1
	}
	return time.Duration((rand.Int()%500+1000)/div) * time.Millisecond
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.logs = make([]LogEntry, 1, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.msgChan = make(chan int, 1)
	rf.applyChan = applyCh

	// go func() {
	// 	rf.applyChan <- ApplyMsg{CommandValid: true, Command: 1, CommandIndex: 0}
	// }()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			rf.mu.Lock()
			isLeader := rf.role == Leader
			rf.mu.Unlock()

			select {
			case <-time.After(randomTimeOut(isLeader)):
				rf.mu.Lock()
				isLeader = rf.role == Leader
				rf.mu.Unlock()

				if isLeader {
					go rf.updateAppliedLock()
					go rf.logDuplicate(nil)
				} else {
					for {
						res := rf.tryToBeLeader()
						DPrintf("server [%d] to  be leader", rf.me)
						if res == BecomeLeader {
							rf.mu.Lock()
							rf.role = Leader
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0
							}
							rf.mu.Unlock()

							go rf.logDuplicate(nil)
							break
						} else if res == BecomeFollower {
							break
						}
					}
				}
			case <-rf.msgChan:
			}
		}
	}()

	return rf
}
