# golang-mastery
A repository of projects I implement to learn the Go programming language.
No paradigm-shifting ideas here. Just me re-implementing existing protocols and applications from scratch to learn about the internal plumbing of modern distributed systems and improve my Go programming skills.

## About
I'm Manuel, a seasoned software engineer with an interest for Cloud technologies and systems programming.
I think the best way to learn a new programming language is studying other people's code and then try to apply the same concepts in your own projects.
This repository represents my learning process, where in every module I challenge myself to use Go features to solve a new problem.

## Projects list
Here's a list of the projects I've been working on. In you're interested in any of them you can find additional information in the specific `README` file.

- [x] [background-task-cancellation](/background-task-cancellation/)
        - learn different ways to handle the cancellation of background tasks running in goroutines
- [x] [concurrency-and-channels](/concurrency-and-channels/)
        - experiment with different Go concurrency patterns and learn how to exchange information between goroutines using channels
- [x] [remote-procedure-call](/remote-procedure-call/)
        - use RPCs (remote procedure calls) to implement a simple application plugin system
- [x] [gossip](/gossip/)
        - implementation of the [Gossip protocol](https://en.wikipedia.org/wiki/Gossip_protocol) to maintain cluster membership information for distributed applications
- [x] [dns-server](/dns-server/)
        - implementation of a toy DNS server capable of serving DNS requests from A-type records stored locally or forwarding requests to authoritative servers upstream
- [x] [distributed-queue](/distributed-queue/) - an implementation of a distributed queue based on [FOQS](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/) (Facebook Ordered Queuing Service) 
- [x] [objects-cache](/objects-cache/) - an in-memory objects cache implementation to support the distributed-queue project


## Future project ideas
* Build a CDN using Go and PostgreSQL
* Implement a distributed queue system
* Add a `testHook` to dns-server application to enable testing of complex scenarios with simulated latency and response failures
* Extend dns-server application to add the ability to cache DNS responses received from upstream servers for their TTL duration
* Refactor gossip protocol to exchange gossip rounds using UDP multicasting to maximise network performance
* Extend the distributed queue with account management. Implement authentication and access control so an account can only interact with its own namespaces.
* Instrument distributed-queue application with OpenTelemetry
