package taskqueue

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	peer "github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
)

const thawSpeed = time.Millisecond * 100

// Executor runs a single task on the queue
type Executor interface {
	ExecuteTask(ctx context.Context, pid peer.ID, task *peertask.Task) bool
}

type TaskQueue interface {
	PushTask(p peer.ID, task peertask.Task)
	TaskDone(p peer.ID, task *peertask.Task)
	Remove(t peertask.Topic, p peer.ID)
	Stats() graphsync.RequestStats
	WithPeerTopics(p peer.ID, f func(*peertracker.PeerTrackerTopics))
}

// WorkerTaskQueue is a wrapper around peertaskqueue.PeerTaskQueue that manages running workers
// that pop tasks and execute them
type WorkerTaskQueue struct {
	lockTopics sync.Mutex
	*peertaskqueue.PeerTaskQueue
	ctx         context.Context
	cancelFn    func()
	workSignal  chan struct{}
	noTaskCond  *sync.Cond
	ticker      *time.Ticker
	activeTasks int32
}

// NewTaskQueue initializes a new queue
func NewTaskQueue(ctx context.Context, ptqopts ...peertaskqueue.Option) *WorkerTaskQueue {
	ctx, cancelFn := context.WithCancel(ctx)
	return &WorkerTaskQueue{
		ctx:           ctx,
		cancelFn:      cancelFn,
		PeerTaskQueue: peertaskqueue.New(ptqopts...),
		workSignal:    make(chan struct{}, 1),
		noTaskCond:    sync.NewCond(&sync.Mutex{}),
		ticker:        time.NewTicker(thawSpeed),
	}
}

// PushTask pushes a new task on to the queue
func (tq *WorkerTaskQueue) PushTask(p peer.ID, task peertask.Task) {
	tq.lockTopics.Lock()
	tq.PeerTaskQueue.PushTasks(p, task)
	tq.lockTopics.Unlock()
	select {
	case tq.workSignal <- struct{}{}:
	default:
	}
}

// TaskDone marks a task as completed so further tasks can be executed
func (tq *WorkerTaskQueue) TaskDone(p peer.ID, task *peertask.Task) {
	tq.lockTopics.Lock()
	tq.PeerTaskQueue.TasksDone(p, task)
	tq.lockTopics.Unlock()
}

// Stats returns statistics about a task queue
func (tq *WorkerTaskQueue) Stats() graphsync.RequestStats {
	tq.lockTopics.Lock()
	ptqstats := tq.PeerTaskQueue.Stats()
	tq.lockTopics.Unlock()
	return graphsync.RequestStats{
		TotalPeers: uint64(ptqstats.NumPeers),
		Active:     uint64(ptqstats.NumActive),
		Pending:    uint64(ptqstats.NumPending),
	}
}

func (tq *WorkerTaskQueue) WithPeerTopics(p peer.ID, withPeerTopics func(*peertracker.PeerTrackerTopics)) {
	tq.lockTopics.Lock()
	peerTopics := tq.PeerTaskQueue.PeerTopics(p)
	withPeerTopics(peerTopics)
	tq.lockTopics.Unlock()
}

// Startup runs the given number of task workers with the given executor
func (tq *WorkerTaskQueue) Startup(workerCount uint64, executor Executor) {
	for i := uint64(0); i < workerCount; i++ {
		go tq.worker(executor)
	}
}

// Shutdown shuts down all running workers
func (tq *WorkerTaskQueue) Shutdown() {
	tq.cancelFn()
}

func (tq *WorkerTaskQueue) WaitForNoActiveTasks() {
	tq.noTaskCond.L.Lock()
	for tq.activeTasks > 0 {
		tq.noTaskCond.Wait()
	}
	tq.noTaskCond.L.Unlock()
}

func (tq *WorkerTaskQueue) worker(executor Executor) {
	targetWork := 1
	for {
		tq.lockTopics.Lock()
		pid, tasks, _ := tq.PeerTaskQueue.PopTasks(targetWork)
		tq.lockTopics.Unlock()
		for len(tasks) == 0 {
			select {
			case <-tq.ctx.Done():
				return
			case <-tq.workSignal:
				tq.lockTopics.Lock()
				pid, tasks, _ = tq.PeerTaskQueue.PopTasks(targetWork)
				tq.lockTopics.Unlock()
			case <-tq.ticker.C:
				tq.lockTopics.Lock()
				tq.PeerTaskQueue.ThawRound()
				pid, tasks, _ = tq.PeerTaskQueue.PopTasks(targetWork)
				tq.lockTopics.Unlock()
			}
		}
		for _, task := range tasks {
			tq.noTaskCond.L.Lock()
			tq.activeTasks = tq.activeTasks + 1
			tq.noTaskCond.L.Unlock()
			terminate := executor.ExecuteTask(tq.ctx, pid, task)
			tq.noTaskCond.L.Lock()
			tq.activeTasks = tq.activeTasks - 1
			if tq.activeTasks == 0 {
				tq.noTaskCond.Broadcast()
			}
			tq.noTaskCond.L.Unlock()
			if terminate {
				return
			}
		}
	}
}
