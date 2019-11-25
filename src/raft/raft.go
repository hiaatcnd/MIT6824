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
	"bytes"
	"github.com/keegancsmith/nth"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

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
const (
	null          = -1
	heartBeatTime = 110 * time.Millisecond
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	UseSnapShot  bool
	Snapshot     []byte
}

type Log struct {
	Term    int
	Command interface{}
}

type Role int

const (
	Follower Role = iota // 0
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     Role
	receiveCh chan struct{}
	killCh    chan struct{}
	applyCh   chan ApplyMsg

	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	currentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int   // candidateId that received vote in current term (or null if none)
	log         []Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// log compaction. In fact, it is the first index of log that not in snapshot
	lastIncludeIndex int
	lastIncludeTerm  int
}

func (rf *Raft) getLog(idx int) Log {
	return rf.log[idx-rf.lastIncludeIndex]
}

func (rf *Raft) LogLen() int {
	return len(rf.log) + rf.lastIncludeIndex
}

// Ensure that the channel with 1 buffer will not be blocked
func sendCh(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
	ch <- struct{}{}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// must be locked before call it
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) <= 0 { // bootstrap without any state?
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, votedFor, lastIncludeIndex, lastIncludeTerm int
	var log []Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		DPrintf("readPersist decode error")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		rf.mu.Unlock()
	}
}

// log compaction and snapshot
func (rf *Raft) Compaction(idx int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx < rf.getLastLogIndex() {
		return
	}
	newLog := append(make([]Log, 0), rf.log[idx-rf.lastIncludeIndex:]...)
	rf.lastIncludeTerm = rf.getLog(idx).Term
	rf.lastIncludeIndex = idx
	rf.log = newLog

	rf.persistWithSnapshot(snapshot)
}

// must be locked before call it
func (rf *Raft) getLastLogIndex() int {
	return rf.LogLen() - 1 // because log of index 0 is useless
}

// must be locked before call it
func (rf *Raft) getLastLogTerm() int {
	return rf.getLog(rf.getLastLogIndex()).Term
}

// must be locked before call it
func (rf *Raft) newThan(index, term int) bool {
	myLogTerm := rf.getLastLogTerm()
	return myLogTerm > term || (myLogTerm == term && rf.getLastLogIndex() > index)
}

// must be locked before call it
// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
func (rf *Raft) checkApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getLog(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("raft %v applied %v: %v", rf.me, rf.lastApplied, applyMsg)
		rf.applyCh <- applyMsg
	}
}

// must be locked before call it
//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) checkCommit() {
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	DPrintf("check matchindex: %v", rf.matchIndex)
	if rf.state != Leader {
		return
	}
	idxs := sort.IntSlice(append(make([]int, 0), rf.matchIndex...))
	mid := (len(rf.peers) - 1) / 2
	nth.Element(idxs, mid)
	if idxs[mid] > rf.commitIndex {
		rf.commitIndex = idxs[mid]
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft %v receive RequestVote: %v", rf.me, args)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		return
	} else if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		rf.beFollower(args.Term)
	}
	if (rf.votedFor == null || rf.votedFor == args.CandidateId) && !rf.newThan(args.LastLogIndex, args.LastLogTerm) {
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		rf.votedFor = args.CandidateId
		rf.persist()
		sendCh(rf.receiveCh)
		reply.VoteGranted = true
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    //	so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	sendCh(rf.receiveCh)

	if args.LastIncludedIndex > rf.lastIncludeIndex {
		newLog := append(make([]Log, 0), rf.log[args.LastIncludedIndex-rf.lastIncludeIndex:]...)
		rf.log = newLog
	}

	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm

	applyMsg := ApplyMsg{UseSnapShot: true, Snapshot: args.Data}
	rf.applyCh <- applyMsg

	rf.persistWithSnapshot(args.Data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // the term of the conflicting entry
	ConflictIndex int  // the first index it stores for that term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("raft %v receive AppendEntries: %v", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = null
	reply.ConflictIndex = null

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	sendCh(rf.receiveCh)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.getLastLogIndex() < args.PrevLogIndex || (args.PrevLogIndex >= rf.lastIncludeIndex && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm) {
		if rf.getLastLogIndex() < args.PrevLogIndex {
			reply.ConflictTerm = null
			reply.ConflictIndex = rf.LogLen()
		} else {
			idx := 1
			reply.ConflictTerm = rf.getLog(args.PrevLogIndex).Term
			for rf.getLog(idx).Term != reply.ConflictTerm {
				idx++
			}
			reply.ConflictIndex = idx
		}
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	j := 0
	for i := args.PrevLogIndex + 1; i < rf.LogLen() && j < len(args.Entries); i, j = i+1, j+1 {
		if i < rf.lastIncludeIndex {
			continue
		}
		if rf.getLog(i).Term != args.Entries[j].Term {
			rf.log = rf.log[:i-rf.lastIncludeIndex]
			break
		}
	}
	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries[j:]...)
	rf.persist()
	if j < len(args.Entries) {
		DPrintf("raft %v logs: %v", rf.me, rf.log)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		rf.checkApplied()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// must be locked before call it
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//DPrintf("raft %v begin election", rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	cnt := int32(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(i, &args, &reply) {
				return
			}
			DPrintf("raft %v receive RequestVoteReply: %v", rf.me, reply)

			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.beFollower(reply.Term)
			}
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&cnt, 1)
			}
			if atomic.LoadInt32(&cnt) > int32(len(rf.peers)/2) {
				rf.beLeader()
				sendCh(rf.receiveCh)
			}
		}(i)
	}
}

// must be locked before call it
func (rf *Raft) beFollower(term int) {
	DPrintf("raft %v become follower", rf.me)
	rf.currentTerm = term
	rf.votedFor = null
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) beCandidate() {
	DPrintf("raft %v become candidate", rf.me)
	rf.mu.Lock()
	rf.state = Candidate
	rf.mu.Unlock()
	go rf.startElection()
}

// must be locked before call it
func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}
	DPrintf("raft %v become leader", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	v := rf.getLastLogIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = v
	}
}

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for {
				if rf.state != Leader {
					break
				}
				DPrintf("raft %v begin to heartbeat to %v, with nextindex: %v", rf.me, i, rf.nextIndex[i])
				if rf.nextIndex[i] <= rf.lastIncludeIndex {

				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.getLog(rf.nextIndex[i] - 1).Term,
					Entries:      append(make([]Log, 0), rf.log[rf.nextIndex[i]-rf.lastIncludeIndex:]...),
					LeaderCommit: rf.commitIndex,
				}
				//DPrintf("raft %v nextindex[%v]: %v", rf.me, i, rf.nextIndex[i])
				tmp := rf.getLastLogIndex()
				//DPrintf("raft %v heartbeat to %v: %v", rf.me, i, args)
				rf.mu.Unlock()
				var reply AppendEntriesReply
				ret := rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				DPrintf("raft %v receive AppendEntriesReply from %v: %v", rf.me, i, reply)
				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				if !ret || rf.state != Leader || rf.currentTerm != args.Term {
					break
				}
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					break
				}
				if reply.Success {
					DPrintf("raft %v nextindex[%v]: %v, len: %v", rf.me, i, rf.nextIndex[i], len(args.Entries))
					rf.nextIndex[i], rf.matchIndex[i] = tmp+1, tmp
					rf.checkCommit()
					rf.checkApplied()
					break
				} else {
					if reply.ConflictIndex != null {
						idx := rf.getLastLogIndex()
						for idx > 0 && rf.getLog(idx).Term != reply.ConflictTerm {
							idx--
						}
						if idx == 0 {
							rf.nextIndex[i] = reply.ConflictIndex
						} else {
							rf.nextIndex[i] = idx + 1
						}
					} else {
						rf.nextIndex[i]--
					}
				}
			}
		}(i)
	}
}

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
	term, isLeader = rf.GetState()
	if !isLeader {
		return null, term, false
	}
	rf.mu.Lock()
	rf.log = append(rf.log, Log{term, command})
	index = rf.getLastLogIndex()
	rf.persist()
	rf.mu.Unlock()

	DPrintf("Receive client request %v", index)
	rf.heartbeat()

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
	DPrintf("raft %v killing...", rf.me)
	sendCh(rf.killCh)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.receiveCh = make(chan struct{}, 1)
	rf.killCh = make(chan struct{}, 1)
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = null
	rf.log = make([]Log, 1) // because the first index is 1, we should have a useless log

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = null

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("begin!")
	go func() {
		defer DPrintf("raft %v killed", rf.me)
		for {
			rf.mu.Lock()
			state := rf.state
			DPrintf("Raft %v New loop with role %v and term %v", rf.me, state, rf.currentTerm)
			rf.mu.Unlock()

			switch state {
			case Follower, Candidate:
				waitTime := time.Duration(rand.Intn(100)+250) * time.Millisecond
				select {
				case <-rf.killCh:
					return
				case <-rf.receiveCh:
					//DPrintf("Raft %v receive ch", rf.me)
				case <-time.After(waitTime):
					rf.beCandidate()
				}
			case Leader:
				rf.heartbeat()
				select {
				case <-rf.killCh:
					return
				case <-time.After(heartBeatTime):
				}
			}
		}
	}()

	return rf
}
