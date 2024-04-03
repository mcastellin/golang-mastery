package main

/*
[cwl:par 00]
---
layout: post
title: "Background Tasks And Cancellation In Go (lang)"
description: >-
  In this article, we'll learn how to run tasks in the background using Go and implement
  graceful termination.
date: 2024-03-28
updated: [cwl:dat curdate]
categories: "golang"
tags: content-creation golang
image: assets/images/gopher.jpg
author: "Manuel Castellin"
permalink: "/golang-background-task-cancellation"
excerpt: >-
  In this article, we'll learn how to run tasks in the background using Go and implement
  graceful termination.
---
[/cwl:par]
*/

/*
[cwl:par 10]
## 1. Overview
Task cancellation is straightforward for iterative background tasks that run inside a `for` loop,
though long-running one-time operations can be trickier.

The main issue with one-shot operations is that they are blocking and don't provide any chance to
bail out prematurely.

#### Example of long-running operation

Let's take a look at this implementation:
[cwl:lnk blockingOp fullSrc=true]

As simple as the `mockComplexOp` is, it represents a real-world scenario where we need
to perform a certain task that is not interruptible. Examples of these tasks are:
- Complex *oneshot* calculations, like rendering tasks
- I/O operations, like reading from files or completing a database transaction
- HTTP interactions with other web servers
[/cwl:par]

[cwl:par 20]
## 2. Background Tasks In Go
[/cwl:par]

[cwl:par 20.1]
The `go` programming language makes it super-easy to run background tasks. In *Golang* they're called
goroutines, and all we need to do to run one is use the `go` keyword, followed by a function call:

```go
func main() {
	op := &mockComplexOp{Duration: time.Second}
	go op.Do()
}
```

One thing to notice is that when we spawn a background goroutine, the Go runtime will take ownership of that task
and we can no longer control its state. The sub-routine will **either run to completion or terminate early
if the main program exits.**

In Go, the only way to interact with a goroutine is by using specific structures called **channels**.

[cwl:lnk runOnce fullSrc=true]

In this example, `runOnce()` is a simple wrapper function that simply calls the `Do()` method on a `longRunningOp`
interface and notify the caller as soon as the operation is completed.

Of course, there are simpler ways of tracking goroutines completion in go (using `sync.WaitGroup`, for example), but
it's good to understand how we can do this ourselves using channels.

In Go, reading from a channel blocks the program execution until new data is pushed into the channel or the channel
itself is closed. In the function above, this means that when the `completedCh` is closed, all processes that are
reading from `completedCh` will be released:

```go
func main() {
	op := &mockComplexOp{Duration: time.Second}
	completed := make(chan struct{})
	go runOpOnce(op, completed)

	// reading from channel will block until the
	// channel is closed by runOpOnce
	<-completed
	fmt.Println("operation completed")
}
```
[/cwl:par]

[cwl:par 30]
## 3. Task Cancellation Using Channels

In the previous section, we learned how **closing a Go channel can be used to send and receive signals between goroutines**.

We can now create a new version of the previous wrapper function that can receive a termination signal on the `cancelCh` channel
and attempt a graceful termination for the operation:

[cwl:lnk cancelWithChannel fullSrc=true]

The first thing this function does is **decouple the long-running operation from the wrapper**. Because the operation is
uninterruptible, we can't handle cancellation if the routine is busy running the operation.

We use the `go` keyword to spawn a new sub-routine and delegate the operation execution. Because this is an *inner
routine*, we also create a dedicated channel to signal the operation completion.

After spawning the sub-routine, we introduced the handling logic for cancellation using the `select-case`.
In this implementation, if the `cancelCh` is closed before the operation sub-routine can complete, we call the `op.Stop()` method
to request graceful termination.



### 3.1. The `select-case` construct in Go

`select-case` is a variation of the `switch-case` construct designed to simplify handling communication over multiple channels.

In essence, every `case` defines a channel interaction, to read/write data from/to a specific channel. The `select` construct
will then activate the `case` that completes the channel interaction first.

Let me explain with an example:

```go
func main() {
	boolCh := make(chan bool)

	b := <-boolCh // permanently blocked!!
	fmt.Printf("Boolean value is %t", b)
}
```

Since nothing is writing data into the `boolCh`, the read operation `case b := <-boolCh` will never complete. As
a result, this program will hang indefinitely.

We can use the `select-case` to implement a timeout on the channel read as follows:

```go
func main() {
	boolCh := make(chan bool)

	select {
	case b := <-boolCh:
		fmt.Printf("Boolean value is %t", b)

	case <-time.After(time.Second):
		// timer will activate first.
		fmt.Println("Operation timed out.")
	}
}
```

In this second example, *the `select` activates the `case` corresponding to the interaction that completes first
(our 1-second timer in this case)* giving us a chance to exit the program if the `boolCh` is inactive for too long.

### 3.2. Cancelling an operation with channel

The last thing we need to do is use the `runOpWithCancelCh` to run a new operation and cancel it before it
can complete:

[cwl:lnk testRunWithCancelCh fullSrc=true]

[/cwl:par]

[cwl:par 40]
## 4. Cancellation Using Context

Since version *1.7*, the `context` package has been introduced into Go's *standard library*. It provides **a way to
manage and propagate cancellation signals, deadlines and request-scoped values** across goroutines.

For our purposes, implementing task cancellation using `Context` is not necessarily simpler, though it provides a
nicer interface for the function caller.

Let's get into it:

[cwl:lnk cancelWithContext  fullSrc=true]

The implementation is almost identical, except for the cancellation `case` in the handling logic.
To receive the cancellation signal from a `context.Context` all we have to do is read from the `ctx.Done()` channel.

And lastly, here is how we call the function to handle cancellation with context:

[cwl:lnk testRunWithContextCancel fullSrc=true]

[/cwl:par]

[cwl:par 99]
## 5. Conclusion

In this article, we learned different ways to gracefully cancel long-running operations running Go subroutines.

As always, all examples used in this post and more are available
<a href="https://github.com/mcastellin/golang-mastery/tree/main/background-task-cancellation" target="_blank">over on GitHub</a>
[/cwl:par]
*/
