package cluster

import (
	"sort"
	"strconv"
	"strings"
)

// Strategy for partition to consumer assignement
type Strategy string

const (
	// StrategyRange is the default and assigns partition ranges to consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 1, 2]
	//   C2: [3, 4, 5]
	StrategyRange Strategy = "range"

	// StrategyRoundRobin assigns partitions by alternating over consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 2, 4]
	//   C2: [1, 3, 5]
	StrategyRoundRobin Strategy = "roundrobin"
)

// Error instances are wrappers for internal errors with a context and
// may be returned through the consumer's Errors() channel
type Error struct {
	Ctx string
	error
}

// --------------------------------------------------------------------

type none struct{}

type topicPartition struct {
	Topic     string
	Partition int32
}

type offsetInfo struct {
	Offset         int64
	PendingOffsets map[int64]struct{}
	Metadata       string
}

func (i offsetInfo) Serialize() offsetInfo {
	meta := ""
	for k, _ := range i.PendingOffsets {
		meta += strconv.FormatInt(k, 10) + ","
	}

	i.Metadata = meta

	return i
}

func (i offsetInfo) Deserialize() offsetInfo {
	parts := strings.Split(i.Metadata, ",")
	for _, k := range parts {
		if k == "" {
			continue
		}

		offset, err := strconv.ParseInt(k, 10, 64)
		// This should NEVER happen!
		if err != nil {
			continue
		}
		i.PendingOffsets[offset] = struct{}{}
	}

	return i
}

func (i offsetInfo) NextOffset(fallback int64) int64 {
	if i.Offset > -1 {
		if len(i.PendingOffsets) > 0 {
			for k, _ := range i.PendingOffsets {
				return k
			}
		}

		return i.Offset
	}
	return fallback
}

type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p int32Slice) Diff(o int32Slice) (res []int32) {
	on := len(o)
	for _, x := range p {
		n := sort.Search(on, func(i int) bool { return o[i] >= x })
		if n < on && o[n] == x {
			continue
		}
		res = append(res, x)
	}
	return
}
