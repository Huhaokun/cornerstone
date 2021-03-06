package leader_election

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Member interface {
	Start()
	Stop()

	IsLeader() bool
}

const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2

type StateChange struct {
	state int
	data  interface{}
}

type member struct {
	client *clientv3.Client

	// static config
	key string
	id  string
	ttl time.Duration

	// dynamic status
	isRunning int32
	isLeader  bool
	stateChan chan StateChange

	rootCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewMember(key string, id string, ttl time.Duration) Member {
	client, err := clientv3.NewFromURL("localhost:2379")
	if err != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &member{
		client:    client,
		key:       key,
		id:        id,
		ttl:       ttl,
		isRunning: 0,
		isLeader:  false,
		stateChan: make(chan StateChange),
		rootCtx:   ctx,
		cancel:    cancel,
	}
}

func (m *member) Start() {
	if !atomic.CompareAndSwapInt32(&m.isRunning, 0, 1) {
		return
	}
	go m.onStateChange()

	m.stateChan <- StateChange{CANDIDATE, time.Duration(0)}
}

func (m *member) Stop() {
	if !atomic.CompareAndSwapInt32(&m.isRunning, 1, 0) {
		return
	}

	m.cancel()

	m.wg.Wait()

	close(m.stateChan)
}

func (m *member) onStateChange() {
	for stateChange := range m.stateChan {

		if m.isRunning == 0 {
			continue
		}

		log.Printf("[node %s] state to %d", m.id, stateChange.state)

		switch stateChange.state {
		case FOLLOWER:
			m.isLeader = false
			go m.runWithWaitGroup(func(ctx context.Context) {
				m.observe(ctx, stateChange.data.(int64))
			})(m.rootCtx)
		case CANDIDATE:
			m.isLeader = false
			// delay execute
			time.AfterFunc(stateChange.data.(time.Duration), func() {
				m.runWithWaitGroup(m.campaign)(m.rootCtx)
			})
		case LEADER:
			m.isLeader = true
			go m.runWithWaitGroup(func(ctx context.Context) {
				m.keepalive(ctx, stateChange.data.(clientv3.LeaseID))
			})(m.rootCtx)
		}
	}
}

func (m *member) runWithWaitGroup(f func(ctx context.Context)) func(ctx context.Context) {
	return func(ctx context.Context) {
		m.wg.Add(1)
		defer m.wg.Done()
		f(ctx)
	}
}

func (m *member) IsLeader() bool {
	return m.isLeader
}

func (m *member) observe(ctx context.Context, rev int64) {
	log.Printf("[node %s] start observe", m.id)
	defer log.Printf("[node %s] stop observe", m.id)
	for resp := range m.client.Watch(ctx, m.key, clientv3.WithRev(rev)) {
		if resp.Err() != nil || resp.Canceled {
			// some error happen, we should stop observe
			return
		}

		// TODO merge events
		for _, event := range resp.Events {
			if event.Type == clientv3.EventTypeDelete {
				m.stateChan <- StateChange{CANDIDATE, time.Duration(0)}
				return
			}
		}
	}
}

func (m *member) campaign(ctx context.Context) {
	log.Printf("[node %s] start campaign", m.id)
	defer log.Printf("[node %s] stop campaign", m.id)

	c, _ := context.WithTimeout(ctx, 5*time.Second)

	lease, err := m.client.Grant(c, int64(m.ttl.Seconds()))
	if err != nil {
		log.Printf("[node %s] grant fail due to error %v", m.id, err)
		// delay it maybe
		m.stateChan <- StateChange{CANDIDATE, time.Second}
		return
	}

	// put if absent
	c2, _ := context.WithTimeout(ctx, 5*time.Second)
	resp, err := m.client.Txn(c2).
		If(clientv3.Compare(clientv3.Version(m.key), "=", 0)).
		Then(clientv3.OpPut(m.key, m.id, clientv3.WithLease(lease.ID))).
		Else(clientv3.OpGet(m.key)).
		Commit()
	if err != nil {
		log.Printf("[node %s] txn fail due to error %v", m.id, err)
		// delay it maybe
		m.stateChan <- StateChange{CANDIDATE, time.Second}
		return
	}

	if resp.Succeeded {
		// become leader
		m.stateChan <- StateChange{LEADER, lease.ID}
	} else {
		// become follower
		watchRev := resp.Responses[0].GetResponseRange().Kvs[0].ModRevision
		m.stateChan <- StateChange{FOLLOWER, watchRev}
	}

}

func (m *member) keepalive(ctx context.Context, leaseId clientv3.LeaseID) {
	log.Printf("[node %s] start keepalive", m.id)
	defer log.Printf("stop keepalive")

	resp, err := m.client.KeepAlive(ctx, leaseId)
	if err != nil {
		log.Printf("[node %s] keep alive failed due to %v", m.id, err)
		m.stateChan <- StateChange{CANDIDATE, time.Duration(0)}
		return
	}

	for range resp {

	}

	m.stateChan <- StateChange{CANDIDATE, time.Duration(0)}
}
