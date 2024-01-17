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
	//	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

//ApplyMsg as each Raft peer becomes aware that successive log entries are
//committed, the peer should send an ApplyMsg to the service (or
//tester) on the same server, via the applyCh passed to Make(). set
//CommandValid to true to indicate that the ApplyMsg contains a newly
//committed log entry.
//
//in part 2D you'll want to send other kinds of messages (e.g.,
//snapshots) on the applyCh, but set CommandValid to false for these
//other uses.
//ApplyMsg当每个Raft对等体意识到连续的日志条目已提交时，
//该对等体应通过传递给Make()的applyCh向同一服务器上的服务（或测试人员）
//发送ApplyMg。将CommandValid设置为true表示ApplyMsg包含一个新提交的日志条目。
//
//在第2D部分中，您将希望在applyCh上发送其他类型的消息（例如快照），
//但对于这些其他用途，将CommandValid设置为false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Log 当前记录的Log日志
type Log struct {
	Command       interface{}
	SnapshotTerm  int // 当前的leader任期
	SnapshotIndex int // 当前的索引
}

// Raft A Go object implementing a single Raft peer.
// 实现单个Raft对等体的Go对象。
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int   // 当前server的任期
	VotedFor    int   // 当前候选者的ID
	Logs        []Log // 日志记录
	CommitIndex int   // 将被提交的日志记录的索引
	LastApplied int   // 已经被提交到状态机的最后一个日志的索引
	NextIndex   []int // 每台机器在数组占据一个元素，元素的值在下调发送到对应的机器的日志索引
	MatchIndex  []int // 将要复制到对应机器日志的索引
	Status      uint8 // 当前的主机的状态

	// 先通过设置过期时间来判断对应的心跳是否过期以及是否需要开启选举
	HBT     int64
	VTT     int64
	HBChain chan struct{}
	VTChain chan struct{}
}

// 状态的枚举值
const (
	Leader    uint8 = iota // 领导者
	Follower               // 更随者
	Candidate              // 候选人状态
)

var rpg = []string{"Leader", "Follower", "Candidate"}

// HEART_BEAT_DELAY 超时事件设置
const (
	HEART_BEAT_DELAY          = 100
	HEART_BEAT_CHECK_DURATION = 3
	VOTE_CHECK_DURATION       = 10
)

// GetState return currentTerm and whether this server
// believes it is the leader.
// GetState返回currentTerm以及此服务器是否认为它是领导者。
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()

	// 当前的状态
	term = rf.CurrentTerm
	isleader = false
	// Your code here (2A).
	if rf.GetStateMutex() == Leader {
		isleader = true
	}
	Debug(dState, strconv.Itoa(rf.me)+"====="+rpg[rf.Status]+"====="+strconv.Itoa(rf.CurrentTerm))
	rf.mu.Unlock()
	return term, isleader
}

///////////////////////////持久化///////////////////////////////////

//save Raft's persistent state to stable storage,
//where it can later be retrieved after a crash and restart.
//see paper's Figure 2 for a description of what should be persistent.
//before you've implemented snapshots, you should pass nil as the
//second argument to persister.Save().
//after you've implemented snapshots, pass the current snapshot
//(or nil if there's not yet a snapshot).
//将Raft的持久状态保存到稳定的存储中，稍后可以在崩溃后在那里检索并重新启动。
//有关应该持久的内容的描述，请参阅本文的图2。
//在实现快照之前，应该将nil作为第二个参数传递给persister。
//Save()。实现快照后，传递当前快照（如果还没有快照，则传递nil）。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Start the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

///////////////////////////停止///////////////////////////////////

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

///////////////////////////发送接受处理心跳///////////////////////////////////

// AppendEntries 定义一个心跳包
type AppendEntries struct {
	Term         int         // leader的任期
	LeaderId     int         // 用来follower重定向到leader
	PrevLogIndex int         // 前继日志记录的索引
	PrevLogItem  int         // 前继日志的任期
	Entries      interface{} // 存储日志记录
	LeaderCommit int         // leader的commitIndex
}

// AppendEntriesResult 定义一个心跳包结果接受
type AppendEntriesResult struct {
	Term    int  // 当前任期，leader用来更新自己
	Success bool // 如果follower包含索引为prevLogIndex和任期为prevLogItem 的日志
}

// LeaderSendHeartBeat 当前leader发送心跳给各个候选人
func (rf *Raft) LeaderSendHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if time.Now().UnixMilli() < rf.HBT || rf.Status != Leader {
			rf.mu.Unlock()
			// 等待3ms进行下一次轮询
			time.Sleep(time.Duration(1) * time.Millisecond)
			continue
		}
		if rf.Status != Leader {
			rf.mu.Unlock()
			return
		}
		// 当前心跳
		rf.HBT = rf.HeartBeatTimeout()
		Watch(dHeart, strconv.Itoa(rf.me)+" start heartbeat next HB "+strconv.Itoa(int(rf.HBT)), rf.HBT)
		// 获取前一个日志的索引以及任期
		lastLog := rf.GetLastLogTermAndIndex()
		// 创建心跳请求当前的心跳请求应该进行定制
		appendEntries := AppendEntries{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: lastLog.SnapshotIndex,
			PrevLogItem:  lastLog.SnapshotTerm,
			Entries:      lastLog.Command,
			LeaderCommit: rf.CommitIndex,
		}
		num := 0
		sum := 1
		rf.mu.Unlock()
		// 循环向所有raft对象发送心跳请求
		for index, peer := range rf.peers {
			if index == rf.me {
				rf.mu.Lock()
				// 修改时间
				rf.VTT = rf.RandomTimeout()
				rf.mu.Unlock()
				continue
			}
			rf.mu.Lock()
			// 判断当前是否是leader
			if rf.Status != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(peer *labrpc.ClientEnd) {
				// 创建接收的对象
				appendEntriesResult := AppendEntriesResult{}
				Debug(dHeart, strconv.Itoa(rf.me)+" Send HB")
				// 发送心跳请求
				ok := peer.Call("Raft.RequestHeartBeat", &appendEntries, &appendEntriesResult)
				Debug(dHeart, strconv.Itoa(rf.me)+" Receive HB")
				// 如果当前的响应全部完成
				rf.mu.Lock()
				sum++
				if sum == len(rf.peers) {
					if !ok {
						num++
					}
					if num > len(rf.peers)/2 && rf.Status == Leader {
						rf.ToFollower("<Heart Beat Off Line>")
						Debug(dHeart, strconv.Itoa(rf.me)+":"+strconv.Itoa(num))
						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
				// 当前响应失效
				if !ok {
					rf.mu.Lock()
					num++
					rf.mu.Unlock()
					return
				}
				// 心跳发送失败,当前的leader退化为follower
				rf.mu.Lock()
				if !appendEntriesResult.Success && appendEntriesResult.Term > rf.CurrentTerm {
					num++
					rf.CurrentTerm = appendEntriesResult.Term
					rf.ToFollower("<Heart Beat Wrong>")
				}
				rf.mu.Unlock()
			}(peer)
		}
	}
}

// RequestHeartBeat 接收到心跳请求之后的处理流程
func (rf *Raft) RequestHeartBeat(input *AppendEntries, output *AppendEntriesResult) {
	rf.mu.Lock()
	// 当前任期较短
	if input.Term < rf.CurrentTerm {
		output.Success = false
		output.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	// 设置当前的节点的角色为Follower
	// todo change for clear log cache
	//rf.ToFollower("<Heart Beat Set>")

	rf.SetStateMutex(Follower)
	rf.VotedFor = -1

	// 重置日期
	rf.VTT = rf.RandomTimeout()
	// 显示接收到心跳
	Debug(dHeart, strconv.Itoa(rf.me)+":"+strconv.Itoa(rf.CurrentTerm)+"<<<<====="+strconv.Itoa(input.LeaderId)+":"+strconv.Itoa(input.Term))
	// 重置任期
	rf.CurrentTerm = input.Term
	output.Success = true
	rf.mu.Unlock()
}

///////////////////////////选主投票部分///////////////////////////////////

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 示例RequestVote RPC参数结构。字段名称必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选者任期
	CandidateId  int // 候选者编号
	LastLogIndex int // 候选者最后一条日志记录的索引
	LastLogItem  int // 候选者最后一日志记录索引的任期
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期，候选者用来更新自己
	VoteGranted bool // 如果候选者当前则为true
}

// RequestVote example RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 当前为Leader或者Candidate那么进入以下的逻辑
	rf.mu.Lock()
	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.VotedFor == -1) {
		rf.ToFollower("<Send Bigger Vote Peer>")
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateId
		// 刷新时间
		rf.VTT = rf.RandomTimeout()
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
}

// SendRequestVote 启动一轮选举，同时自己的term需要+1
func (rf *Raft) SendRequestVote() {
	for !rf.killed() {
		rf.mu.Lock()
		if time.Now().UnixMilli() < rf.VTT {
			// 等待3ms进行下一次轮询
			time.Sleep(time.Duration(VOTE_CHECK_DURATION) * time.Millisecond)
			rf.mu.Unlock()
			continue
		}
		if !(rf.GetStateMutex() == Candidate || rf.GetStateMutex() == Follower) {
			rf.mu.Unlock()
			continue
		}
		// 刷新时间
		rf.VTT = rf.RandomTimeout()
		// 变成角色
		rf.ToCandidate("<Start Vote>")
		// 更新当前的任期
		rf.CurrentTerm++
		// 首先需要投给自己一票
		sum := 1
		rf.VotedFor = rf.me
		rf.mu.Unlock()
		// 征求其他人意见
		for index, peer := range rf.peers {
			// 跳过自己
			if index == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.Status != Candidate {
				// debug
				Debug(dVote, strconv.Itoa(rf.me)+"-> !VOTE")
				rf.mu.Unlock()
				return
			}
			// 初始化数值
			log := rf.GetLastLogTermAndIndex()
			requestVoteArgs := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: log.SnapshotIndex,
				LastLogItem:  log.SnapshotTerm,
			}
			rf.mu.Unlock()
			go func(index int, peer *labrpc.ClientEnd) {
				requestVoteReply := RequestVoteReply{}
				ok := peer.Call("Raft.RequestVote", &requestVoteArgs, &requestVoteReply)
				// debug
				Debug(dVote, strconv.Itoa(index)+"->"+strconv.Itoa(rf.me)+" ["+strconv.FormatBool(ok)+"]["+strconv.FormatBool(requestVoteReply.VoteGranted)+"]["+strconv.Itoa(requestVoteReply.Term)+"]")
				if !ok {
					return
				}
				rf.mu.Lock()
				// 当前把票投给本机
				if requestVoteReply.VoteGranted {
					sum++
					// 大多数同意
					if sum > len(rf.peers)/2 && rf.Status == Candidate {
						// 将当前的任期改为Leader
						rf.ToLeader("<Vote Win>")
						rf.mu.Unlock()
						return
					}
				}
				// 当前出现任期更大的对象直接退回
				if requestVoteReply.Term > rf.CurrentTerm {
					rf.CurrentTerm = requestVoteReply.Term
					rf.ToFollower("<Get Bigger Vote Peer>")
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}(index, peer)
		}
	}
}

///////////////////////////工具方法///////////////////////////////////

// GetLastLogTermAndIndex 这个函数目的是获取当前插入的最后的任期以及索引
func (rf *Raft) GetLastLogTermAndIndex() Log {
	if len(rf.Logs) == 0 {
		return Log{
			Command:       nil,
			SnapshotTerm:  -1,
			SnapshotIndex: -1,
		}
	}
	return rf.Logs[len(rf.Logs)-1]
}

// GetStateMutex 线程安全的方式获取当前的raft节点状态
func (rf *Raft) GetStateMutex() uint8 {
	return rf.Status
}

// SetStateMutex 线程安全的方式设置当前raft的状态
func (rf *Raft) SetStateMutex(state uint8) {
	rf.Status = state
}

// RandomTimeout 设置随机的超时时间
func (rf *Raft) RandomTimeout() int64 {
	return time.Now().UnixMilli() + 300 + rand.Int63n(150)
}

// HeartBeatTimeout 设置随机的超时时间
func (rf *Raft) HeartBeatTimeout() int64 {
	return time.Now().UnixMilli() + HEART_BEAT_DELAY
}

///////////////////////////角色转换方法///////////////////////////////////

// ToFollower 角色转换从leader转换为follower
func (rf *Raft) ToFollower(line string) {
	// debug
	Debug(dDrop, line+":"+strconv.Itoa(rf.me)+"["+rpg[rf.Status]+"]->[Follower]")
	rf.SetStateMutex(Follower)
	rf.VotedFor = -1
}

// ToLeader 角色转换从follower转换为leader
func (rf *Raft) ToLeader(line string) {
	// debug
	Debug(dDrop, line+":"+strconv.Itoa(rf.me)+"["+rpg[rf.Status]+"]->[Leader]")
	rf.SetStateMutex(Leader)
	rf.VotedFor = -1
}

// ToCandidate 角色转换从CandidateTo转换为Follower
func (rf *Raft) ToCandidate(line string) {

	// debug
	Debug(dDrop, line+":"+strconv.Itoa(rf.me)+"["+rpg[rf.Status]+"]->[Candidate]")

	rf.VotedFor = -1
	rf.SetStateMutex(Candidate)
}

///////////////////////////主函数///////////////////////////////////

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Status = Follower
	rf.VTChain = make(chan struct{})
	rf.HBChain = make(chan struct{})
	rf.VotedFor = -1
	// 设置当前的毫秒数
	rf.HBT = rf.HeartBeatTimeout()
	rf.VTT = rf.RandomTimeout()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.LeaderSendHeartBeat()
	go rf.SendRequestVote()

	// debug
	Debug(dVote, strconv.Itoa(me)+"-> [Init]")
	return rf
}
