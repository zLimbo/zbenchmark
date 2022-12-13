package main

import (
	"fmt"
	"math/rand"
	"time"
	"zbenchmark/smallbank"
	"zbenchmark/ycsb"
)

func TestTime() {
	t0 := time.Now()
	txSet := ycsb.GenTxSet()
	txs := ycsb.EncodeTxSet(txSet)
	take1 := time.Since(t0)
	fmt.Println("gen take:", take1)
	t1 := time.Now()
	n := ycsb.ExecTxSet(txs)
	take2 := time.Since(t1)
	fmt.Println("exec take:", take2)
	fmt.Println("tx num:", n)

	cnt := [2]int{0, 0}
	for _, tx := range txSet {
		for _, op := range tx.Ops {
			cnt[op.Type]++
		}
	}
	fmt.Println(cnt)
}

func TestConflicit() {
	ycsb.Init()
	a := ycsb.GenTxSet()
	b := ycsb.GenTxSet()
	c := ycsb.GenTxSet()
	d := ycsb.GenTxSet()

	println("a, b, c, d ======= ")
	ycsb.SolveConflict(a, b, c, d)

	println("a, b, d, c ======= ")
	ycsb.SolveConflict(a, b, d, c)
}

func TestNormalRandom() {
	n := 100
	u := 10.0
	c := 1.0
	mm := map[int]int{}
	for i := 0; i < n; i++ {
		x := int(rand.NormFloat64()*c + u)
		mm[x]++
	}

	for k, v := range mm {
		fmt.Println(k, v)
	}
}

func TestSmallbank() {
	s := smallbank.NewSmallbank("smallbank_levdb", ycsb.KConfig.OriginKeys)
	a := s.GenTxSet(ycsb.KConfig.BatchTxNum)
	b := s.GenTxSet(ycsb.KConfig.BatchTxNum)
	c := s.GenTxSet(ycsb.KConfig.BatchTxNum)
	d := s.GenTxSet(ycsb.KConfig.BatchTxNum)

	println("a, b, c, d ======= ")
	ycsb.SolveConflict(a, b, c, d)

	println("a, b, d, c ======= ")
	ycsb.SolveConflict(a, b, d, c)
}

func TestSallbankDAG() {
	s := smallbank.NewSmallbank("dag_levdb", ycsb.KConfig.OriginKeys)
	a := s.GenTxSet(ycsb.KConfig.BatchTxNum)
	b := s.GenTxSet(ycsb.KConfig.BatchTxNum)

	ycsb.SolveConflictWithDAG(a, b)
	// c := s.GenTxSet(ycsb.KConfig.BatchTxNum)
	// d := s.GenTxSet(ycsb.KConfig.BatchTxNum)

	// println("a, b, c, d ======= ")
	// ycsb.SolveConflict(a, b, c, d)

	// println("a, b, d, c ======= ")
	// ycsb.SolveConflict(a, b, d, c)
}

func TestConflicitDiff() {
	t0 := time.Now()
	s := smallbank.NewSmallbank("diff_levdb", ycsb.KConfig.OriginKeys)
	initTake := time.Since(t0)
	fmt.Println("init take:", initTake)

	var take1, take2, take3, take4 time.Duration
	var cnt1, cnt2, cnt3, cnt4 int
	n := 10
	for i := 0; i < n; i++ {
		a := s.GenTxSet(ycsb.KConfig.BatchTxNum)
		b := s.GenTxSet(ycsb.KConfig.BatchTxNum)

		t0 = time.Now()
		cnt1 += ycsb.Conflicit22Count(a, b)
		take1 += time.Since(t0)

		t0 = time.Now()
		cnt3 += ycsb.Conflicit22CountParaller(a, b)
		take3 += time.Since(t0)

		t0 = time.Now()
		cnt2 += ycsb.ConflicitKeyCount(a, b)
		take2 += time.Since(t0)

		t0 = time.Now()
		cnt4 += ycsb.ConflicitKeyCountParaller(a, b)
		take4 += time.Since(t0)
	}

	fmt.Printf("cnt1:%d, take1:%v\n", cnt1, take1)
	fmt.Printf("cnt2:%d, take2:%v\n", cnt2, take2)
	fmt.Printf("cnt3:%d, take3:%v\n", cnt3, take3)
	fmt.Printf("cnt4:%d, take4:%v\n", cnt4, take4)
}

func main() {
	// rand.Seed(time.Now().Unix())
	// TestConflicit()
	// TestNormalRandom()
	// TestTime()

	// TestSmallbank()

	// TestSallbankDAG()
	// TestConflicitDiff()

	
}
