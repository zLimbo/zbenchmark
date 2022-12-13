package ycsb

import (
	"sync"
	"sync/atomic"
)

// 两两交易检查
func Conflicit22Count(a []*Tx, b []*Tx) int {
	conflicitTxSet := map[*Tx]struct{}{}

	for _, txa := range a {
		txaWKeys := map[string]struct{}{}
		for _, op := range txa.Ops {
			// if op.Type == OpWrite {
			txaWKeys[op.Key] = struct{}{}
			// }
		}
		have := false
	LoopForTxb:
		for _, txb := range b {
			for _, op := range txb.Ops {
				if _, ok := txaWKeys[op.Key]; ok {
					conflicitTxSet[txb] = struct{}{}
					have = true
					continue LoopForTxb
				}
			}
		}
		if have {
			conflicitTxSet[txa] = struct{}{}
		}
	}

	return len(conflicitTxSet)
}

func GenKey2Txs(txs []*Tx) map[string]map[*Tx]struct{} {
	key2Txs := map[string]map[*Tx]struct{}{}

	for _, tx := range txs {
		for _, op := range tx.Ops {
			if _, ok := key2Txs[op.Key]; !ok {
				key2Txs[op.Key] = make(map[*Tx]struct{})
			}
			key2Txs[op.Key][tx] = struct{}{}
		}
	}

	return key2Txs
}

func ConflicitKeyCount(a []*Tx, b []*Tx) int {
	aKey2Txs := GenKey2Txs(a)
	bKey2Txs := GenKey2Txs(b)
	if len(bKey2Txs) < len(aKey2Txs) {
		aKey2Txs, bKey2Txs = bKey2Txs, aKey2Txs
	}

	conflicitTxSet := map[*Tx]struct{}{}
	for akey, atxs := range aKey2Txs {
		if btxs, ok := bKey2Txs[akey]; ok {
			for tx := range atxs {
				conflicitTxSet[tx] = struct{}{}
			}
			for tx := range btxs {
				conflicitTxSet[tx] = struct{}{}
			}
		}
	}

	return len(conflicitTxSet)
}

func Conflicit22CountParaller(a []*Tx, b []*Tx) int {
	// conflicitTxSet := map[*Tx]int{}
	// for _, txa := range a {
	// 	conflicitTxSet[txa] = 0
	// }
	// for _, txb := range b {
	// 	conflicitTxSet[txb] = 0
	// }

	wg := sync.WaitGroup{}
	wg.Add(len(a))
	acnt := int32(0)

	for _, txa := range a {
		txa1 := txa
		go func() {
			txaWKeys := map[string]struct{}{}
			for _, op := range txa1.Ops {
				// if op.Type == OpWrite {
				txaWKeys[op.Key] = struct{}{}
				// }
			}
			have := false
		LoopForTxb:
			for _, txb := range b {
				for _, op := range txb.Ops {
					if _, ok := txaWKeys[op.Key]; ok {
						have = true
						// conflicitTxSet[txb] = 1
						atomic.AddInt32(&acnt, 1)
						continue LoopForTxb
					}
				}
			}
			if have {
				// conflicitTxSet[txa1] = 1
				atomic.AddInt32(&acnt, 1)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	// cnt := 0
	// for _, txa := range a {
	// 	cnt += conflicitTxSet[txa]
	// }
	// for _, txb := range b {
	// 	cnt += conflicitTxSet[txb]
	// }

	// return cnt
	return int(acnt)
}

func ConflicitKeyCountParaller(a []*Tx, b []*Tx) int {
	wg := sync.WaitGroup{}
	wg.Add(2)
	var aKey2Txs, bKey2Txs map[string]map[*Tx]struct{}
	go func() {
		aKey2Txs = GenKey2Txs(a)
		wg.Done()
	}()
	go func() {
		bKey2Txs = GenKey2Txs(b)
		wg.Done()
	}()
	wg.Wait()

	// aKey2Txs := GenKey2Txs(a)
	// bKey2Txs := GenKey2Txs(b)
	if len(bKey2Txs) < len(aKey2Txs) {
		aKey2Txs, bKey2Txs = bKey2Txs, aKey2Txs
	}

	// acnt := int32(0)
	conflicitTxSet := map[*Tx]struct{}{}

	// wg = sync.WaitGroup{}
	// wg.Add(len(aKey2Txs))
	for akey, atxs := range aKey2Txs {
		// akey1 := akey
		// atxs1 := atxs
		// go func() {
		if btxs, ok := bKey2Txs[akey]; ok {
			for tx := range atxs {
				conflicitTxSet[tx] = struct{}{}
				// atomic.AddInt32(&acnt, 1)
			}
			for tx := range btxs {
				conflicitTxSet[tx] = struct{}{}
				// atomic.AddInt32(&acnt, 1)
			}
		}
		// wg.Done()
		// }()
	}
	// wg.Wait()
	return len(conflicitTxSet)
	// return int(acnt)
}
