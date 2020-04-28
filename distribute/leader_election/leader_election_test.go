package leader_election

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMember_IsLeader(t *testing.T) {
	member1 := NewMember("/election", "1", 3 * time.Second)

	member2 := NewMember("/election", "2", 3 * time.Second)

	member1.Start()

	member2.Start()

	time.Sleep(1 * time.Second)

	assert.True(t, member1.IsLeader() || member2.IsLeader())

	member1.Stop()

	assert.False(t, member1.IsLeader())

	time.Sleep(4 * time.Second)

	assert.True(t, member2.IsLeader())
}