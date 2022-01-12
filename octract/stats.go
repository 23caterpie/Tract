package octract

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// Worker stats

	WorkerWorkLatency = stats.Float64(
		"octract/worker/work/latency",
		"worker work latency",
		stats.UnitMilliseconds,
	)
	WorkerWaitLatency = stats.Float64(
		"octract/worker/wait/latency",
		"worker wait latency",
		stats.UnitMilliseconds,
	)
	WorkerInputLatency = stats.Float64(
		"octract/worker/input/latency",
		"input latency",
		stats.UnitMilliseconds,
	)
	WorkerOutputLatency = stats.Float64(
		"octract/worker/output/latency",
		"output latency",
		stats.UnitMilliseconds,
	)

	// Group stats

	GroupWorkLatency = stats.Float64(
		"octract/group/work/latency",
		"group work latency",
		stats.UnitMilliseconds,
	)
)

var (
	WorkerName = tag.MustNewKey("worker.name")
	GroupName  = tag.MustNewKey("group.name")
)

var (
	DefaultLatencyDistribution = view.Distribution(1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
)

var (
	// Worker views

	WorkerWorkLatencyView = &view.View{
		Name:        "octract/worker/work/latency",
		Description: "Latency distribution of worker work",
		TagKeys:     []tag.Key{WorkerName},
		Measure:     WorkerWorkLatency,
		Aggregation: DefaultLatencyDistribution,
	}
	WorkerWaitLatencyView = &view.View{
		Name:        "octract/worker/wait/latency",
		Description: "Latency distribution of worker wait",
		TagKeys:     []tag.Key{WorkerName},
		Measure:     WorkerWaitLatency,
		Aggregation: DefaultLatencyDistribution,
	}
	WorkerInputLatencyView = &view.View{
		Name:        "octract/worker/input/latency",
		Description: "Latency distribution of worker inputs",
		TagKeys:     []tag.Key{WorkerName},
		Measure:     WorkerInputLatency,
		Aggregation: DefaultLatencyDistribution,
	}
	WorkerOutputLatencyView = &view.View{
		Name:        "octract/worker/output/latency",
		Description: "Latency distribution of worker outputs",
		TagKeys:     []tag.Key{WorkerName},
		Measure:     WorkerOutputLatency,
		Aggregation: DefaultLatencyDistribution,
	}

	// Group views

	GroupWorkLatencyView = &view.View{
		Name:        "octract/group/work/latency",
		Description: "Latency distribution of group work",
		TagKeys:     []tag.Key{GroupName},
		Measure:     GroupWorkLatency,
		Aggregation: DefaultLatencyDistribution,
	}
)

func RegisterDefaultViews() {
	view.Register(
		WorkerWorkLatencyView,
		WorkerWaitLatencyView,
		WorkerInputLatencyView,
		WorkerOutputLatencyView,
		GroupWorkLatencyView,
	)
}