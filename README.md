# Raft KV数据库

## 总览

本项目基于Raft共识协议实现了一个强一致性的KV数据库，同时实现了数据库的分片和动态迁移功能。 Raft协议是工程上使用比较广泛的分布式协议，并且相比同为共识算法的Paxos更加易懂。 本项目基于[Raft(extended)论文](https://pages.cs.wisc.edu/~remzi/Classes/739/Spring2004/Papers/raft.pdf)的描述实现了Raft共识算法，并以此为基础在上层实现了支持分片的KV数据库，达到了线性读写的强一致性。

本项目主要分为3个部分：

* Raft协议层：`src/raft`中实现
* 基于Raft协议层的分片元数据管理服务：`src/shardctrler`中实现
* 基于Raft协议层的分片读写服务：`src/shardkv`中实现

总体视图如下：
![](pic/RaftKV.JPG)

分片元数据管理服务类似于GFS中Master的角色，记录了从服务开始以来所有版本的配置，每一个版本的配置记录了整个服务中的所有群组以及其内的机器，还记录了分片所属的群组，Client和KV Server可以使用Query接口查询任意版本的配置信息，Admin可以通过Join、Leave、Move接口生成新版本的配置，接口描述如下：
* Query(cid)：查询第cid版本的配置信息
* Join(gid):加入id为gid的KV组并均衡shard产生新配置
* Leave(gid):下掉id为gid的KV组并均衡shard产生新配置
* Move(shardId, gid):将分片id为shardId的分片迁移至gid代表的KV组产生新配置

分片读写服务则负责其上所有分片的读写(Get/Put/Append接口)，同时由于分片配置的变更，需要定时Query元数据管理服务，获得当前配置的下一个配置，有新的配置时，依据新老配置的差异向负责分片的旧Group发送拉取分片PullShard的请求，并在拉取完成后发送删除分片DeleteShard的请求，接口描述如下：
* Get(key)：获得key对应的value
* Put(key, val)：将key的值设置为val
* Append(key, val)：将key的值追加val
* PullShard(cid, shardIds)：告知所处的配置版本cid，需要拉取shardIds中分片id对应的分片数据
* DeleteShard(cid, shardIds)：已经拉取完成，可以让旧group删除shardIds中分片id对应的数据，释放内存

以上所有与写相关的操作，因为可能有网络不稳定、重传等情况发生，要在服务端做幂等过滤，本项目通过记录所有客户端id的最大命令Index来过滤已经处理过的请求并直接返回结果，进而达到线性读写，不会因为网络问题产生roll back的现象，详见后文。

所有服务执行任何操作都需要先在Raft层达成共识，以此达到强一致性，服务通过Start函数向Raft层发起对某个操作达成共识的请求，Raft层通过一个管道告知服务操作已被提交，同时也提供了相应的函数来为服务的状态做快照，详见后文。

## Raft协议层

Raft协议的实现主要依据原论文中的描述，RPC接口的汇总描述如原论文的Figure 2所示：
![](pic/Raft_RPC.png)

### 节点状态定义

```go
type Raft struct {
	mu        sync.Mutex          //本节点内读写字段的互斥锁，防止竞争
	peers     []*labrpc.ClientEnd //共识组的所有成员RPC客户端
	persister *Persister          //持久化本节点状态的存储器
	me        int                 //本节点在共识组中的编号
	dead      int32               //可以被Kill()函数设置，标记宕机，方便测试

	currentTerm int        //本节点所处Term(变更需要持久化)
	votedFor    int        //本节点投票给候选者的编号，-1代表没投(变更需要持久化)
	log         []LogEntry //存储所有log项(变更需要持久化)，log[0]存储快照的lastIncludedTerm、lastIncludedIndex
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

### Leader选举
为了初始时或者Leader宕机时能够及时选出新的Leader，在启动每个节点的时候，会为每个节点启动一个ticker协程，做为Follower的节点，如果长时间没有收到Leader的心跳或者其他消息，在一定时间后，electionTimer会触发本节点的ticker发起新一轮的选举，切换状态到Candidate，并启动一个协程，异步地向其他所有节点发送RequestVote请求，并在获得一半以上选票后成为leader。该部分实现在`src/raft/raft.go`的ticker和startElection函数中。

RequestVote接口用于Candidate向其他节点请求选举投票，主要依据原论文描述实现，详见`src/raft/raft.go`的RequestVote接口。

### 日志项复制
为了Leader能够不阻塞地向所有Follower同步已有的log项，采用了为每个Follower都创建一个协程的方式，每个协程只负责自己对应的Follower的日志项复制，通过检查对应Follower的nextIndex和Leader拥有的最大日志项Index决定需不需要向Follower同步日志项，以及同步多少日志项。为了避免资源浪费，在不满足同步条件时，协程会进入阻塞状态，每当Leader有新的日志项时便会被唤醒检查是否需要同步。该部分实现在`src/raft/raft.go`的replicator函数中。

AppendEntries接口用于Leader向Follower复制日志，主要依据原论文描述实现，详见`src/raft/raft.go`的AppendEntries接口。

replicator函数通过调用sendOneEntryToPeer函数真正的向Follower发送数据，sendOneEntryToPeer函数中会根据要发的日志项索引决定是发送快照(InstallSnapshot)还是日志项(AppendEntries)，并构造相应的请求参数，同时对于AppendEntries还会通过回复信息实时检查是否大于一半的Follower接收到日志项，进而决定能否提交。该部分实现在`src/raft/raft.go`的sendOneEntryToPeer函数中。

假如Leader提交(commit)日志项的同时应用(apply)了日志项，那么由于复制日志项有多个并发协程，可能同一时刻向上层服务的管道中塞了多个相同的日志项，为了分离日志项的提交(commit)和应用(apply)，剥离出了applier协程，通过检查已apply的索引和已commit的索引，不断追赶其间的日志项，同样为了避免资源浪费，在不满足同步条件时，协程会进入阻塞，当commitIndex有更新时将唤醒applier检查，从而不断地向上层服务apply日志项。该部分实现在`src/raft/raft.go`的applier函数中。

### 快照
同时为了避免基本Raft算法日志项无限增长的问题，本项目实现了快照功能，其中节点重启从leader快速恢复到最新状态的安装快照RPC接口如原论文的Figure 13所示：
![](pic/Raft_install_snapshot.png)
上层服务可以通过Snapshot函数为将上层服务状态形成的快照发给Raft节点，函数中Raft节点可以截断日志长度，释放内存，并将快照和自己的状态进行持久化，达到节约内存的效果。该部分是现在`src/raft/raft.go`的Snapshot函数中。

InstallSnapshot接口用于Leader向Follower发送快照，接受方会向上层服务管道发送快照快速达到快照状态，主要依据原论文描述实现，详见`src/raft/raft.go`的InstallSnapshot接口。

## 分片元数据管理服务

### 服务端

#### 节点状态定义
```go
type ShardCtrler struct {
	mu      sync.Mutex         //本节点内读写字段的互斥锁，防止竞争
	me      int                //本节点在共识组中的编号
	rf      *raft.Raft         //本节点下的Raft节点
	applyCh chan raft.ApplyMsg //接收Raft节点apply的日志项

	configs      []Config                   //所有版本的配置信息
	duplicateMap map[int64]LastContext      //存每一个clientId上一个请求的commandId和reply
	waitApply    map[int]chan *CommandReply //每一次Command请求，用来等待apply后返回给客户端
}
```

#### 处理命令
Command

#### 应用命令
applier

### 客户端

## 分片读写服务

### 服务端

#### 节点状态定义
分片状态机

#### 处理读写命令
KVCommand

#### 更新分片配置
updater

#### 处理拉取分片命令
PullShard

puller

#### 处理删除分片命令
DeleteShard

deleter

#### 空命令
emptySender


### 客户端