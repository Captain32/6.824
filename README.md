# Raft KV数据库

## 总览

本项目基于Raft共识协议实现了一个强一致性的KV数据库，同时实现了数据库的分片和动态迁移功能。 Raft协议是工程上使用比较广泛的分布式协议，并且相比同为共识算法的Paxos更加易懂。 本项目基于[Raft(extended)论文](https://pages.cs.wisc.edu/~remzi/Classes/739/Spring2004/Papers/raft.pdf)的描述实现了Raft共识算法，并以此为基础在上层实现了支持分片的KV数据库，达到了线性读写的强一致性。

本项目主要分为3个部分：

* Raft协议层：`src/raft`中实现
* 基于Raft协议层的分片元数据管理服务：`src/shardctrler`中实现
* 基于Raft协议层的分片读写服务：`src/shardkv`中实现

总体视图如下：

## Raft协议层

Raft协议的实现主体依据原论文中的描述，主要RPC接口的汇总描述如原论文的Figure 2所示：
![](pic/Raft_RPC.png)

同时为了避免基本Raft算法日志项无限增长的问题，本项目实现了快照功能，其中节点重启从leader快速恢复到最新状态的安装快照RPC接口如原论文的Figure 13所示：
![](pic/Raft_install_snapshot.png)

Raft层的总体实现视图如下：

Raft节点的结构体定义如下：
```go
type Raft struct {
	mu        sync.Mutex          //本节点内读写字段的互斥锁，防止竞争
	peers     []*labrpc.ClientEnd //共识组的所有成员RPC客户端
	persister *Persister          //持久化本节点状态的存储器
	me        int                 //本节点在共识组中的编号
	dead      int32               //可以被Kill()函数设置，标记宕机，方便测试

	currentTerm int        //本节点所处Term
	votedFor    int        //本节点投票给候选者的编号，-1代表没投
	log         []LogEntry //存储所有log项，log[0]存储快照的lastIncludedTerm、lastIncludedIndex
	commitIndex int        //本节点已经commit的log项索引
	lastApplied int        //本节点已经apply的log项索引
	nextIndex   []int      //本节点做为Leader时，用于管理给某个Follower下一个该发的日志项索引
	matchIndex  []int      //本节点做为Leader时，用于管理已知某个Follower已经接收到的最大日志项索引

	state          State         //当前角色，Leader、Follower、Candidate
	applyCh        chan ApplyMsg //向上层服务发送已commit日志项的管道
	applyCond      *sync.Cond    //用于异步激活applier
	replicateConds []*sync.Cond  //用于异步激活对应某个Follower的replicator

	electionTimer  *time.Timer //发起选举计时器
	heartbeatTimer *time.Timer //leader发送心跳计时器
}
```
