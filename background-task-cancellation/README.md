# background-task-cancellation

In this module we experiment with goroutine cancellation.

Task cancellation is straightforward for iterative background tasks that run inside a `for` loop, though long-running one-time operations can be trickier. The main issue with one-shot operations is that they are blocking and don't provide any chance to bail out prematurely.
