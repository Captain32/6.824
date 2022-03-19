package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type State int

const (
	StateLeader State = iota
	StateCandidate
	StateFollower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry //log[0]存储快照的lastIncludedTerm、lastIncludedIndex
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state          State //当前角色
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   //用于异步激活applier
	replicateConds []*sync.Cond //用于异步激活replicator

	electionTimer  *time.Timer //发起选举计时器
	heartbeatTimer *time.Timer //leader发送心跳计时器
}

func electionTimeout() time.Duration {
	return time.Duration(850+rand.Intn(150)) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	return 105 * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == StateLeader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("Decode Error! ")
		return
	}
	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.mu.Unlock()
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool { //上层服务通知raft已经应用了snapshot，raft需要相应更新applied、commitId等
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex { //没有自己已提交的新，过时消息
		return false
	}

	DPrintf("Term %v: Server %v cond Install Snapshot last index %v, last term %v\n", rf.currentTerm, rf.me, lastIncludedIndex, lastIncludedTerm)

	tmpLog := []LogEntry{{
		Term:  lastIncludedTerm,
		Index: lastIncludedIndex,
	}}
	if lastIncludedIndex < rf.log[len(rf.log)-1].Index { //原本log还有tail
		tmpLog = append(tmpLog, rf.log[lastIncludedIndex-rf.log[0].Index+1:]...)
	}
	rf.log = tmpLog
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	tmpLog := []LogEntry{} //新分配，旧的可以被GC
	tmpLog = append(tmpLog, rf.log[index-rf.log[0].Index:]...)
	tmpLog[0].Command = nil
	rf.log = tmpLog
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	DPrintf("Term %v: Server %v Snapshot last index %v, new log[0]:%v, len(log):%v\n", rf.currentTerm, rf.me, index, rf.log[0], len(rf.log))
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //比自己的term小，拒绝
		reply.Term = rf.currentTerm
		return
	}

	DPrintf("Term %v: Server %v receive snapshot from Server %v with Term %v\n", rf.currentTerm, rf.me, args.LeaderId, args.Term)
	if args.Term > rf.currentTerm { //比自己的term大，更新自己的term
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.state = StateFollower                  //切换状态到Follower(原本可能是Candidate或者老Leader)
	rf.electionTimer.Reset(electionTimeout()) //重置选举超时时间
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex { //没有自己已提交的新，apply可能会和自己的applier冲突，比如snapshot快照到100，但是applier已经到120了，那么状态机需要回退来应用快照
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //比自己的term小，拒绝
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	DPrintf("Term %v: Server %v receive request vote %v from Server %v with Term %v\n", rf.currentTerm, rf.me, args, args.CandidateId, args.Term)
	if args.Term > rf.currentTerm { //比自己的term大，更新自己的term，新一轮选举变为follower，选票为空
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //还没投票，或者已经投给该候选者
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) { //候选者最后一条日志更新，给候选者投票
			DPrintf("Term %v: Server %v vote for Server %v\n", rf.currentTerm, rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId            //投票
			rf.electionTimer.Reset(electionTimeout()) //重置选举计时
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.persist()
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.state = StateCandidate
	rf.votedFor = rf.me //给自己投票
	rf.persist()
	voteNum := 1
	DPrintf("Term %v: Server %v start election\n", rf.currentTerm, rf.me)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state == StateCandidate && rf.currentTerm == reply.Term && reply.VoteGranted { //自己是本轮候选者并收到选票
					voteNum++
					DPrintf("Term %v: Server %v receive a new vote, has %v votes now, need %v votes\n", rf.currentTerm, rf.me, voteNum, len(rf.peers)/2+1)
					if voteNum > len(rf.peers)/2 { //检查是否已经过半
						DPrintf("Term %v: Server %v becomes new leader\n", rf.currentTerm, rf.me)
						rf.state = StateLeader
						for j := range rf.peers { //初始化两个leader数组
							rf.nextIndex[j] = len(rf.log) + rf.log[0].Index
							rf.matchIndex[j] = 0
						}
						rf.broadcastHeartbeat(true)
						rf.heartbeatTimer.Reset(heartbeatTimeout())
					}
				} else if rf.currentTerm < reply.Term { //自己term落后了需要更新，转换状态
					rf.state = StateFollower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
				}
			}
		}(i)
	}
	rf.electionTimer.Reset(electionTimeout()) //需要重置，不然会卡在此状态
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //比自己的term小，拒绝
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	DPrintf("Term %v: Server %v receive AppendEntries request %v from Server %v with Term %v\n", rf.currentTerm, rf.me, args, args.LeaderId, args.Term)
	if args.Term > rf.currentTerm { //比自己的term大，更新自己的term
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.state = StateFollower                  //切换状态到Follower(原本可能是Candidate或者老Leader)
	rf.electionTimer.Reset(electionTimeout()) //重置选举超时时间

	if args.PrevLogIndex < rf.log[0].Index { //比自己快照小，不应该
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex { //自己的log不够长
		DPrintf("Term %v: Leader %v {prevTerm:%v prevIndex:%v} Server %v log too short {len:%v startIdx:%v}\n", rf.currentTerm, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.me, len(rf.log), rf.log[0].Index)
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} else if rf.log[args.PrevLogIndex-rf.log[0].Index].Term != args.PrevLogTerm { //prevLog不匹配
		DPrintf("Term %v: Leader %v {prevTerm:%v prevIndex:%v} UnMatch Server %v log {prevTerm:%v prevIndex:%v}\n", rf.currentTerm, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.me, rf.log[args.PrevLogIndex-rf.log[0].Index].Term, args.PrevLogIndex)
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
		index := args.PrevLogIndex - 1
		for index >= rf.log[0].Index && rf.log[index-rf.log[0].Index].Term == reply.ConflictTerm { //找到该冲突term的起点
			index--
		}
		reply.ConflictIndex = index + 1
		reply.Term, reply.Success = rf.currentTerm, false
		rf.log = rf.log[:args.PrevLogIndex-rf.log[0].Index] //把冲突部分截断
		return
	}

	DPrintf("Term %v: Server %v len(log)=%v before append\n", rf.currentTerm, rf.me, len(rf.log))
	for i, entry := range args.Entries { //遍历找第一个不同entry(rf目标位置为空或者term不一致)
		if entry.Index-rf.log[0].Index >= len(rf.log) || rf.log[entry.Index-rf.log[0].Index].Term != entry.Term {
			rf.log = append(rf.log[:entry.Index-rf.log[0].Index], args.Entries[i:]...)
			break
		}
	}
	//不能直接这样截断再append，假如网络延迟，先发出的请求后到，就会让log回退！！！
	//rf.log = append(rf.log[:args.PrevLogIndex-rf.startIndex+1], args.Entries...)
	rf.persist()
	DPrintf("Term %v: Server %v len(log)=%v after persist\n", rf.currentTerm, rf.me, len(rf.log))

	newCommitIndex := Min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-rf.log[0].Index].Term == rf.currentTerm {
		//更新commitIdx，必须对应位置是本轮term的entry，因为leader不能commit前任的entry，只能通过自己的顺带commit前任
		//假如A的log为{0_1,1_1,2_1,3_1}，B的log为{0_1,1_1,2_1,4_1}，其中A是第三轮leader，给自己加了3_1后断掉，随后B成为第四轮leader
		//由于B的nextIndex[A]是3(初始化的时候)，Prev都是2_1，所以心跳无法查出A目前log中冲突的3_1，反而因为携带了LeaderCommit，导致A把
		//3_1提交apply，和B提交的4_1不一致！！！
		DPrintf("Term %v: Leader %v commitIdx %v > Server %v commitIdx %v\n", rf.currentTerm, args.LeaderId, args.LeaderCommit, rf.me, rf.commitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Broadcast() //告诉applier应用日志
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendOneEntryToPeer(server int, heartbeatFlag bool, done chan bool) { //为server发送一条entry，heartbeatFlag为true则为心跳，done用来告知调用者rpc已成功
	rf.mu.Lock()
	//发送消息是异步的，这时候可能已不是leader，需要检查，否则有可能两个分区的leader(假设一个A term 1，一个B term 2)重新连接后
	// 1.B给A发送心跳，A接受后变成follower，并且term变成2
	// 2.由于A收到B心跳前(仍是leader时)，可能已经开始广播自己的心跳，由于是用go异步调用本函数，到这里运行的时候已经不是leader但依然发送了心跳(并且由于第一步，这里心跳的term是2！！！)
	// 3.B收到第2步A发的心跳，发现和自己term相同，变成了follower，并且把自己的log根据A发送的prevLogIndex截断(可能会变短)
	// 4.B在第3步退出leader前可能已经开始广播心跳，在这里发送心跳会根据peer的nextIndex设置Prev信息，由于第三步存在log被截断的可能，所以这里可能会越界！！！
	//因此一定要判断是否是leader，只有leader才可以构造请求参数，然后发送请求
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	DPrintf("Term %v: Server %v next index is %v\n", rf.currentTerm, server, rf.nextIndex[server])
	if rf.nextIndex[server] <= rf.log[0].Index { //要发送的entry已经在快照里了，需要InstallSnapshot
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log[0].Index,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(server, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("Term %v: Leader %v receive InstallSnapshot reply %v from Server %v\n", rf.currentTerm, rf.me, reply, server)
			if rf.state == StateLeader && rf.currentTerm == reply.Term { //本机是当前term的leader
				rf.nextIndex[server] = Max(rf.nextIndex[server], args.LastIncludedIndex+1)
			} else if rf.currentTerm < reply.Term {
				rf.state = StateFollower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
			}
		}
	} else { //通过AppendEntries发送
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1-rf.log[0].Index].Term,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}

		if !heartbeatFlag {
			args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]-rf.log[0].Index:]...)
			DPrintf("Term %v: Server %v send log entry %v to Server %v\n", rf.currentTerm, rf.me, args.Entries, server)
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		if rf.sendAppendEntries(server, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("Term %v: Leader %v receive AppendEntries reply %v from Server %v\n", rf.currentTerm, rf.me, reply, server)
			if rf.state == StateLeader && rf.currentTerm == reply.Term { //本机是当前term的leader
				if reply.Success && !heartbeatFlag && len(args.Entries) != 0 { //发送成功，更新nextIndex、matchIndex
					rf.nextIndex[server] = Max(rf.nextIndex[server], args.Entries[len(args.Entries)-1].Index+1)
					rf.matchIndex[server] = Max(rf.matchIndex[server], args.Entries[len(args.Entries)-1].Index)
					if rf.matchIndex[server] > rf.commitIndex { //检查是否有新的commit
						for idx := rf.matchIndex[server]; idx > rf.commitIndex; idx-- { //倒着检查，遇到就代表可以commit，可以改进为找matchIndex的中位数，就是commitIndex
							if rf.log[idx-rf.log[0].Index].Term != rf.currentTerm { //leader只能commit自己任期内的entry
								break
							}
							if rf.checkCommit(idx) {
								rf.commitIndex = idx
								DPrintf("Term %v: Server %v commit log entry %v\n", rf.currentTerm, rf.me, rf.commitIndex)
								rf.applyCond.Broadcast() //告诉applier应用日志
								break
							}
						}
					}
				} else if !reply.Success { //发送失败，server的nextIndex更新
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else { //TODO:区分leader有没有ConflictTerm
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
			} else if rf.currentTerm < reply.Term {
				rf.state = StateFollower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
			}
			if !heartbeatFlag {
				done <- true
			}
		} else {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("Term %v: Server %v send log entry %v to Server %v failed\n", rf.currentTerm, rf.me, args.Entries, server)
		}
	}
}

func (rf *Raft) broadcastHeartbeat(heartbeatFlag bool) {
	if heartbeatFlag {
		DPrintf("Term %v: Server %v broadcast heartbeat\n", rf.currentTerm, rf.me)
	} else {
		DPrintf("Term %v: Server %v broadcast append entry\n", rf.currentTerm, rf.me)
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if heartbeatFlag {
			go rf.sendOneEntryToPeer(i, heartbeatFlag, nil)
		} else {
			DPrintf("Term %v: Server %v replicator to Sever %v cond broadcast\n", rf.currentTerm, rf.me, i)
			rf.replicateConds[i].Broadcast()
		}
	}
}

func (rf *Raft) checkCommit(idx int) bool { //检查idx是否可以commit
	agreements := 0
	for _, num := range rf.matchIndex {
		if num >= idx {
			agreements++
			if agreements > len(rf.peers)/2 {
				return true
			}
		}
	}
	return false
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return -1, -1, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   len(rf.log) + rf.log[0].Index,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.matchIndex[rf.me] = entry.Index //记得给自己标记...
	rf.broadcastHeartbeat(false)

	DPrintf("Term %v: Server %v process new request entry %v\n", rf.currentTerm, rf.me, entry)

	return entry.Index, entry.Term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) HasCurrentLog() bool { //当前log中是否有本term的entry，没有的话上层服务需要发送空日志避免活锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm == rf.log[len(rf.log)-1].Term
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select { //不能先区分是否是leader分别监听，因为if判断state和其内部的动作不是原子的
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.broadcastHeartbeat(true)
				rf.electionTimer.Reset(electionTimeout())
				rf.heartbeatTimer.Reset(heartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		tmpMe := rf.me
		applyEntries := rf.log[rf.lastApplied-rf.log[0].Index+1 : rf.commitIndex-rf.log[0].Index+1]
		rf.mu.Unlock() //释放锁，后续用复制品applyEntries，避免阻塞其他函数

		for _, entry := range applyEntries {
			DPrintf("Term %v: Server %v apply entry %v\n", entry.Term, tmpMe, entry.Index) //防止Data Race
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, applyEntries[len(applyEntries)-1].Index) //更新lastApplied，可能与快照apply并发
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicator(server int) { //循环把数据同步给server
	for rf.killed() == false {
		rf.mu.Lock()
		for !(rf.state == StateLeader && rf.matchIndex[server] < rf.log[len(rf.log)-1].Index) { //不需要补齐
			rf.replicateConds[server].Wait()
			DPrintf("Term %v: Server %v replicator to Server %v check, matchIndex %v, lastLogIdx %v\n", rf.currentTerm, rf.me, server, rf.matchIndex[server], rf.log[len(rf.log)-1].Index)
		}
		DPrintf("Term %v: Server %v replicator to Server %v start\n", rf.currentTerm, rf.me, server)
		rf.mu.Unlock()
		done := make(chan bool, 1)
		go rf.sendOneEntryToPeer(server, false, done) //用go异步，因为发送失败call会阻塞5s，很容易测试超时，这里要么超时1.5s直接重试，要么成功直接continue
		select {
		case <-done:
			continue
		case <-time.After(150 * time.Millisecond):
			continue
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		log:            []LogEntry{{Term: 0, Index: 0}},
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          StateFollower,
		applyCh:        applyCh,
		replicateConds: make([]*sync.Cond, len(peers)),
		electionTimer:  time.NewTimer(electionTimeout()),
		heartbeatTimer: time.NewTimer(heartbeatTimeout()),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied, rf.commitIndex = rf.log[0].Index, rf.log[0].Index
	DPrintf("Term %v: Make Server %v\n", rf.currentTerm, rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	//异步apply entry
	go rf.applier()

	for i := range rf.peers { //为每个peer启动replicator
		if i == rf.me {
			continue
		}
		rf.replicateConds[i] = sync.NewCond(&rf.mu)
		go rf.replicator(i)
	}

	return rf
}
