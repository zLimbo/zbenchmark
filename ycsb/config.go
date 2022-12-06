package ycsb

type Config struct {
	BatchTxNum int
	ValueSize  int
	OpsPerTx   int
	OriginKeys int
	WRate      float64
	HotKey     float64
	HotKeyRate float64
	StdDiff    float64
}

var KConfig = Config{
	BatchTxNum: 1000,
	ValueSize:  64,
	OpsPerTx:   3,
	OriginKeys: 10000,
	WRate:      0.5,
	HotKey:     0.2,
	HotKeyRate: 0.6,
	StdDiff:    10000.0,
}
