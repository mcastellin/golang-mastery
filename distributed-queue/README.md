# distributed-queue

An implementation of a distributed queue based on [FOQS](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/) (Facebook Ordered Queuing Service) 

**WIP**: This project is a Work In Progress:
- [x] Initial implementation of database structure and workers
- [x] API server
- [x] Namespace in-memory cache with invalidation and eviction
- [ ] Message ack/nack lease
- [ ] Shard management at runtime with scaling out of shards
- [ ] Memory management for prefetch buffer to evict expired messages from the buffer
