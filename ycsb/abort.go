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
