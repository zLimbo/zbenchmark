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

func main() {
	// rand.Seed(time.Now().Unix())
	// TestConflicit()
	// TestNormalRandom()
	// TestTime()

	TestSmallbank()

	TestSallbankDAG()
}
