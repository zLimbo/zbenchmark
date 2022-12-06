package ycsb

import "fmt"

type BlockInfo struct {
	key2txs map[string]map[*Tx]struct{}
	abort   map[*Tx]struct{}
}

func NewBlockInfo() *BlockInfo {
	return &BlockInfo{
		key2txs: make(map[string]map[*Tx]struct{}),
		abort:   map[*Tx]struct{}{},
	}
}

func SolveConflict(a []*Tx, oth ...[]*Tx) {
	key2txs := map[string]map[*Tx]struct{}{}
	for _, tx := range a {
		for _, op := range tx.Ops {
			// 读操作跳过，写和更新记录
			if op.Type == OpRead {
				continue
			}
			if key2txs[op.Key] == nil {
				key2txs[op.Key] = make(map[*Tx]struct{})
			}
			key2txs[op.Key][tx] = struct{}{}
			// fmt.Println("a key", op.Key)
		}
	}
	tot := len(a)
	abortNum := 0
	for _, b := range oth {
		abort := map[*Tx]struct{}{}
	LoopTx:
		for _, tx := range b {
			for _, op := range tx.Ops {
				if op.Type == OpRead {
					continue
				}
				if _, ok := key2txs[op.Key]; ok {
					abort[tx] = struct{}{}
					continue LoopTx
				}
			}
		}
		for _, tx := range b {
			if _, ok := abort[tx]; ok {
				continue
			}
			for _, op := range tx.Ops {
				// 读操作跳过，写和更新记录
				if op.Type == OpRead {
					continue
				}
				if key2txs[op.Key] == nil {
					key2txs[op.Key] = make(map[*Tx]struct{})
				}
				key2txs[op.Key][tx] = struct{}{}
			}
		}
		tot += len(b)
		abortNum += len(abort)
		fmt.Println("\na keys", len(key2txs))
		fmt.Println("b abort", len(abort))
	}
	abortRate := float64(abortNum) / float64(tot)
	fmt.Printf("\ntotal tx: %d, abort tx: %d, rest tx: %d, abortRate: %.3f\n", tot, abortNum, tot-abortNum, abortRate)
}


type Dag struct {
	dag map[*Tx]map[*Tx]struct{}
	abort  map[*Tx]struct{}
}

func NewDag(txs []*Tx) *Dag {
	dag := map[*Tx]map[*Tx]struct{}{}
	key2lastTx := map[string]*Tx{}
	for _, tx := range txs {
		for _, op := range tx.Ops {
			if u, ok := key2lastTx[op.Key]; ok {
				if _, ok1 := dag[u]; !ok1 {
					dag[u] = make(map[*Tx]struct{})
				}
				dag[u][tx] = struct{}{}
			}
			key2lastTx[op.Key] = tx
		}
	}
	return &Dag{
		dag: dag,
		abort: make(map[*Tx]struct{}),
	}
}

func (d *Dag) dfsAbort(u *Tx) {
	if _, isAbort := d.abort[u]; isAbort {
		return
	}
	d.abort[u] = struct{}{}
	for v := range d.dag[u] {
		d.dfsAbort(v)
	}
}

func (d *Dag) NoAbortChildNum(u *Tx) int {
	if _, isAbort := d.abort[u]; isAbort {
		return 0
	}
	sum := 1
	for v := range d.dag[u] {
		sum += d.NoAbortChildNum(v)
	}
	return sum
}

func SolveConflictWithDAG(a []*Tx, oth ...[]*Tx) {

	// aDag := NewDag(a)
	for _, b := range oth {
		bDag := NewDag(b)
		abortTxs := map[*Tx]struct{}{}
		for _, txa := range a {
			wkeys := map[string]struct{}{}
			for _, op := range txa.Ops {
				if op.Type == OpWrite {
					wkeys[op.Key] = struct{}{}
				}
			}
			if len(wkeys) == 0 {
				continue
			}
			for _, txb := range b {
				if _, isAbort := abortTxs[txb]; isAbort {
					continue
				}
				for _, op := range txb.Ops {
					if _, exist := wkeys[op.Key]; exist {
						bDag.dfsAbort(txb)
					}
				}
			}
		}
		nAbort := len(abortTxs)
		nAll := len(b)
		fmt.Printf("all: %d, abort: %d\n", nAll, nAbort)
	}
}
