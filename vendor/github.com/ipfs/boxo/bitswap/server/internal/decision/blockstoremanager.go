package decision

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	bstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-metrics-interface"
)

// blockstoreManager maintains a pool of workers that make requests to the blockstore.
type blockstoreManager struct {
	bs           bstore.Blockstore
	workerCount  int
	jobs         chan func()
	pendingGauge metrics.Gauge
	activeGauge  metrics.Gauge

	workerWG sync.WaitGroup
	stopChan chan struct{}
	stopOnce sync.Once
}

// newBlockstoreManager creates a new blockstoreManager with the given context
// and number of workers
func newBlockstoreManager(
	bs bstore.Blockstore,
	workerCount int,
	pendingGauge metrics.Gauge,
	activeGauge metrics.Gauge,
) *blockstoreManager {
	return &blockstoreManager{
		bs:           bs,
		workerCount:  workerCount,
		jobs:         make(chan func()),
		pendingGauge: pendingGauge,
		activeGauge:  activeGauge,
		stopChan:     make(chan struct{}),
	}
}

func (bsm *blockstoreManager) start() {
	bsm.workerWG.Add(bsm.workerCount)
	for i := 0; i < bsm.workerCount; i++ {
		go bsm.worker()
	}
}

func (bsm *blockstoreManager) stop() {
	bsm.stopOnce.Do(func() {
		close(bsm.stopChan)
	})
	bsm.workerWG.Wait()
}

func (bsm *blockstoreManager) worker() {
	defer bsm.workerWG.Done()
	for {
		select {
		case <-bsm.stopChan:
			return
		case job := <-bsm.jobs:
			bsm.pendingGauge.Dec()
			bsm.activeGauge.Inc()
			job()
			bsm.activeGauge.Dec()
		}
	}
}

func (bsm *blockstoreManager) addJob(ctx context.Context, job func()) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-bsm.stopChan:
		return errors.New("shutting down")
	case bsm.jobs <- job:
		bsm.pendingGauge.Inc()
		return nil
	}
}

func (bsm *blockstoreManager) getBlockSizes(ctx context.Context, ks []cid.Cid) (map[cid.Cid]int, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	sizes := make([]int, len(ks))

	var count atomic.Int32
	err := bsm.jobPerKey(ctx, ks, func(i int, c cid.Cid) {
		size, err := bsm.bs.GetSize(ctx, c)
		if err != nil {
			if !ipld.IsNotFound(err) {
				// Note: this isn't a fatal error. We shouldn't abort the request
				log.Errorf("blockstore.GetSize(%s) error: %s", c, err)
			}
			return
		}
		sizes[i] = size
		count.Add(1)
	})
	if err != nil {
		return nil, err
	}
	results := count.Load()
	if results == 0 {
		return nil, nil
	}

	res := make(map[cid.Cid]int, results)
	for i, n := range sizes {
		if n != 0 {
			res[ks[i]] = n
		}
	}
	return res, nil
}

func (bsm *blockstoreManager) hasBlocks(ctx context.Context, ks []cid.Cid) (map[cid.Cid]struct{}, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	hasBlocks := make([]bool, len(ks))

	var count atomic.Int32
	err := bsm.jobPerKey(ctx, ks, func(i int, c cid.Cid) {
		has, err := bsm.bs.Has(ctx, c)
		if err != nil {
			// Note: this isn't a fatal error. We shouldn't abort the request
			log.Errorf("blockstore.Has(%c) error: %s", c, err)
			return
		}
		if has {
			hasBlocks[i] = true
			count.Add(1)
		}
	})
	if err != nil {
		return nil, err
	}
	results := count.Load()
	if results == 0 {
		return nil, nil
	}

	res := make(map[cid.Cid]struct{}, results)
	for i, ok := range hasBlocks {
		if ok {
			res[ks[i]] = struct{}{}
		}
	}
	return res, nil
}

func (bsm *blockstoreManager) getBlocks(ctx context.Context, ks []cid.Cid) (map[cid.Cid]blocks.Block, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	blks := make([]blocks.Block, len(ks))

	var count atomic.Int32
	err := bsm.jobPerKey(ctx, ks, func(i int, c cid.Cid) {
		blk, err := bsm.bs.Get(ctx, c)
		if err != nil {
			if !ipld.IsNotFound(err) {
				// Note: this isn't a fatal error. We shouldn't abort the request
				log.Errorf("blockstore.Get(%s) error: %s", c, err)
			}
			return
		}
		blks[i] = blk
		count.Add(1)
	})
	if err != nil {
		return nil, err
	}
	results := count.Load()
	if results == 0 {
		return nil, nil
	}

	res := make(map[cid.Cid]blocks.Block, results)
	for i, blk := range blks {
		if blk != nil {
			res[ks[i]] = blk
		}
	}
	return res, nil
}

func (bsm *blockstoreManager) jobPerKey(ctx context.Context, ks []cid.Cid, jobFn func(i int, c cid.Cid)) error {
	if len(ks) == 1 {
		jobFn(0, ks[0])
		return nil
	}

	var err error
	var wg sync.WaitGroup
	for i, k := range ks {
		c := k
		idx := i
		wg.Add(1)
		err = bsm.addJob(ctx, func() {
			jobFn(idx, c)
			wg.Done()
		})
		if err != nil {
			wg.Done()
			break
		}
	}
	wg.Wait()
	return err
}
