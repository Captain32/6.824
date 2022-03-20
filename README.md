# Raft KV数据库

## 总览

本项目基于Raft共识协议达到一致性的KV数据库，同时实现了数据库的分片和动态迁移功能。 Raft协议是工程上使用比较广泛的分布式协议，并且相比同为共识算法的Paxos更加易懂。 本项目基于[Raft(extended)论文](https://pages.cs.wisc.edu/~remzi/Classes/739/Spring2004/Papers/raft.pdf)的描述实现了Raft共识算法，并以此为基础在上层实现了支持分片的KV数据库，达到了线性读写的强一致性。