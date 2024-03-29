module github.com/mcastellin/golang-mastery/distributed-queue

go 1.21

require (
	github.com/lib/pq v1.10.9
	github.com/mcastellin/golang-mastery/objects-cache v0.0.0
	github.com/rs/xid v1.5.0
	go.uber.org/zap v1.27.0
)

require go.uber.org/multierr v1.10.0 // indirect

replace github.com/mcastellin/golang-mastery/objects-cache => ../objects-cache
