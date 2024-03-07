# distributed-queue

An implementation of a distributed queue based on [FOQS](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/) (Facebook Ordered Queuing Service) 


## Journal

- first experiment had very poor performance because we stored queue information only in one shard, hence creating a bottleneck in one database with CPU spikes at 300%. Removing the queue exists verification brought us to an ingestion speed of 400k messages per minute.

