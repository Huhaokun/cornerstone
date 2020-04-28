package leader_election

import (
	"context"
	"github.com/etcd-io/etcd/clientv3"
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
		stateChan: make(chan StateChange),
		rootCtx:   ctx,
		cancel:    cancel,
	}
}

func (m *member) Start() {
	if !atomic.CompareAndSwapInt32(&m.isRunning, 0, 1) {
		return
	}
	m.onStateChange()
	go m.campaign(m.rootCtx)
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

		log.Printf("state to %d", stateChange.state)

		switch stateChange.state {
		case FOLLOWER:
			go m.runWithWaitGroup(func(ctx context.Context) {
				m.observe(ctx, stateChange.data.(int64))
			})(m.rootCtx)
		case CANDIDATE:
			// delay execute
			time.AfterFunc(stateChange.data.(time.Duration), func() {
				m.runWithWaitGroup(m.campaign)(m.rootCtx)
			})
		case LEADER:
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
	log.Printf("start observe")
	defer log.Printf("stop observe")
	for resp := range m.client.Watch(ctx, m.key, clientv3.WithRev(rev)) {
		if resp.Err() != nil || resp.Canceled {
			// some error happen, we should stop observe
			return
		}

		// TODO merge events
		for _, event := range resp.Events {
			if event.Type == clientv3.EventTypeDelete {
				m.stateChan <- StateChange{CANDIDATE, 0}
				return
			}
		}
	}
}

func (m *member) campaign(ctx context.Context) {
	log.Printf("start campaign")
	defer log.Printf("stop campaign")

	c, _ := context.WithTimeout(ctx, 5*time.Second)

	lease, err := m.client.Grant(c, m.ttl.Nanoseconds())
	if err != nil {
		log.Printf("grant fail due to error %v", err)
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
		log.Printf("txn fail due to error %v", err)
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
	log.Printf("start keepalive")
	defer log.Printf("stop keepalive")

	resp, err := m.client.KeepAlive(ctx, leaseId)
	if err != nil {
		log.Printf("keep alive failed due to %v", err)
		m.stateChan <- StateChange{CANDIDATE, 0}
		return
	}

	for range resp {

	}

	m.stateChan <- StateChange{CANDIDATE, 0}
}
