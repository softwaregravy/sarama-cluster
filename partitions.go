package cluster

import (
	"sort"
	"sync"

	"github.com/segmentio/sarama"
)

type partitionConsumer struct {
	pcm sarama.PartitionConsumer

	state partitionState
	mutex sync.Mutex

	closed      bool
	dying, dead chan none
}

func newPartitionConsumer(manager sarama.Consumer, topic string, partition int32, info offsetInfo, defaultOffset int64) (*partitionConsumer, error) {
	pcm, err := manager.ConsumePartition(topic, partition, info.NextOffset(defaultOffset))

	// Resume from default offset, if requested offset is out-of-range
	if err == sarama.ErrOffsetOutOfRange {
		info.Offset = -1
		pcm, err = manager.ConsumePartition(topic, partition, defaultOffset)
	}
	if err != nil {
		return nil, err
	}

	return &partitionConsumer{
		pcm:   pcm,
		state: partitionState{Info: info},

		dying: make(chan none),
		dead:  make(chan none),
	}, nil
}

func (c *partitionConsumer) Loop(messages chan<- *sarama.ConsumerMessage, errors chan<- error) {
	defer close(c.dead)

	for {
		select {
		case msg, ok := <-c.pcm.Messages():
			if !ok {
				return
			}
			select {
			case messages <- msg:
			case <-c.dying:
				return
			}
		case err, ok := <-c.pcm.Errors():
			if !ok {
				return
			}
			select {
			case errors <- err:
			case <-c.dying:
				return
			}
		case <-c.dying:
			return
		}
	}
}

func (c *partitionConsumer) Close() error {
	if c.closed {
		return nil
	}

	err := c.pcm.Close()
	c.closed = true
	close(c.dying)
	<-c.dead

	return err
}

func (c *partitionConsumer) State() partitionState {
	if c == nil {
		return partitionState{}
	}

	c.mutex.Lock()
	state := c.state
	if state.Info.Metadata == "" {
		state.Info = state.Info.Serialize()
	} else if len(state.Info.PendingOffsets) == 0 {
		state.Info = state.Info.Deserialize()
	}

	c.mutex.Unlock()

	return state
}

func (c *partitionConsumer) MarkCommitted(offset int64) {
	if c == nil {
		return
	}

	c.mutex.Lock()
	if offset == c.state.Info.Offset || c.state.Info.Metadata != "" {
		c.state.Dirty = false
	}
	c.mutex.Unlock()
}

func (c *partitionConsumer) AddPendingOffset(offset int64) {
	c.state.Info.PendingOffsets[offset] = struct{}{}
}

func (c *partitionConsumer) SetOffset(offset int64) {
	c.pcm.SetOffset(offset)
}

func (c *partitionConsumer) RemovePendingOffset(offset int64) {
	delete(c.state.Info.PendingOffsets, offset)
}

func (c *partitionConsumer) MarkOffset(offset int64, metadata string) {
	if c == nil {
		return
	}

	c.mutex.Lock()
	if offset > c.state.Info.Offset {
		c.state.Info.Offset = offset
		// Only commit metadata if it's a valid string.
		if metadata != "" {
			c.state.Info.Metadata = metadata
		}
		c.state.Dirty = true
	}
	c.mutex.Unlock()
}

// --------------------------------------------------------------------

type partitionState struct {
	Info  offsetInfo
	Dirty bool
}

// --------------------------------------------------------------------

type partitionMap struct {
	data  map[topicPartition]*partitionConsumer
	mutex sync.RWMutex
}

func newPartitionMap() *partitionMap {
	return &partitionMap{
		data: make(map[topicPartition]*partitionConsumer),
	}
}

func (m *partitionMap) Fetch(topic string, partition int32) *partitionConsumer {
	m.mutex.RLock()
	pc, _ := m.data[topicPartition{topic, partition}]
	m.mutex.RUnlock()
	return pc
}

func (m *partitionMap) Store(topic string, partition int32, pc *partitionConsumer) {
	m.mutex.Lock()
	m.data[topicPartition{topic, partition}] = pc
	m.mutex.Unlock()
}

func (m *partitionMap) HasDirty() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for _, pc := range m.data {
		if state := pc.State(); state.Dirty {
			return true
		}
	}
	return false
}

func (m *partitionMap) Snapshot() map[topicPartition]partitionState {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	snap := make(map[topicPartition]partitionState, len(m.data))
	for tp, pc := range m.data {
		snap[tp] = pc.State()
	}
	return snap
}

func (m *partitionMap) Stop() {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	wg := new(sync.WaitGroup)
	for tp := range m.data {
		wg.Add(1)
		go func(p *partitionConsumer) {
			_ = p.Close()
			wg.Done()
		}(m.data[tp])
	}
	wg.Wait()
}

func (m *partitionMap) Clear() {
	m.mutex.Lock()
	for tp := range m.data {
		delete(m.data, tp)
	}
	m.mutex.Unlock()
}

func (m *partitionMap) Info() map[string][]int32 {
	info := make(map[string][]int32)
	m.mutex.RLock()
	for tp := range m.data {
		info[tp.Topic] = append(info[tp.Topic], tp.Partition)
	}
	m.mutex.RUnlock()

	for topic := range info {
		sort.Sort(int32Slice(info[topic]))
	}
	return info
}
