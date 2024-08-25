package decision

import (
	"context"
	"errors"
	"sync"

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
	res := make(map[cid.Cid]int)
	if len(ks) == 0 {
		return res, nil
	}

	var lk sync.Mutex
	return res, bsm.jobPerKey(ctx, ks, func(c cid.Cid) {
		size, err := bsm.bs.GetSize(ctx, c)
		if err != nil {
			if !ipld.IsNotFound(err) {
				// Note: this isn't a fatal error. We shouldn't abort the request
				log.Errorf("blockstore.GetSize(%s) error: %s", c, err)
			}
		} else {
			lk.Lock()
			res[c] = size
			lk.Unlock()
		}
	})
}

func (bsm *blockstoreManager) getBlocks(ctx context.Context, ks []cid.Cid) (map[cid.Cid]blocks.Block, error) {
	res := make(map[cid.Cid]blocks.Block, len(ks))
	if len(ks) == 0 {
		return res, nil
	}

	var lk sync.Mutex
	return res, bsm.jobPerKey(ctx, ks, func(c cid.Cid) {
		blk, err := bsm.bs.Get(ctx, c)
		if err != nil {
			if !ipld.IsNotFound(err) {
				// Note: this isn't a fatal error. We shouldn't abort the request
				log.Errorf("blockstore.Get(%s) error: %s", c, err)
			}
			return
		}

		lk.Lock()
		res[c] = blk
		lk.Unlock()
	})
}

func (bsm *blockstoreManager) jobPerKey(ctx context.Context, ks []cid.Cid, jobFn func(c cid.Cid)) error {
	var err error
	var wg sync.WaitGroup
	for _, k := range ks {
		c := k
		wg.Add(1)
		err = bsm.addJob(ctx, func() {
			jobFn(c)
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
