package kreconciler

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"sync"
	"testing"
	"time"
)

type entry struct {
	t time.Time
	v string
}

type rcv struct {
	sync.Mutex
	items []entry
}

func (r *rcv) OnItem(t time.Time, i interface{}) {
	r.Lock()
	defer r.Unlock()
	r.items = append(r.items, entry{t: t, v: i.(string)})
}

func assertRcv(t *testing.T, r *rcv, elts ...string) {
	r.Lock()
	defer r.Unlock()
	var items []string
	var times []time.Time
	for _, v := range r.items {
		items = append(items, v.v)
		times = append(times, v.t)
	}
	assert.Equal(t, elts, items)
	assert.True(t, sort.SliceIsSorted(times, func(i, j int) bool {
		return times[i].Before(times[j])
	}), "Items didn't trigger in good order")
}

func TestDelay(t *testing.T) {
	t.Parallel()
	t.Run("delay 0", func(t *testing.T) {
		dq := newQueue(10, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("1", "1", 0))

		time.Sleep(time.Millisecond * 20)
		cancel()
		assertRcv(t, r, "1")
	})
	t.Run("delay long then short", func(t *testing.T) {
		dq := newQueue(10, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("1", "1", time.Millisecond*50))
		require.NoError(t, dq.schedule("2", "2", time.Millisecond*20))

		time.Sleep(time.Millisecond * 100)
		cancel()
		assertRcv(t, r, "2", "1")
	})
	t.Run("delay same order", func(t *testing.T) {
		dq := newQueue(10, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("1", "1", time.Millisecond*20))
		require.NoError(t, dq.schedule("2", "2", time.Millisecond*40))

		time.Sleep(time.Millisecond * 100)
		cancel()
		assertRcv(t, r, "1", "2")
	})
	t.Run("delay goes to empty and grows again", func(t *testing.T) {
		dq := newQueue(2, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("1", "1", time.Millisecond*20))
		require.NoError(t, dq.schedule("2", "2", time.Millisecond*40))
		time.Sleep(time.Millisecond * 100)
		assertRcv(t, r, "1", "2")

		require.NoError(t, dq.schedule("3", "3", time.Millisecond*20))
		require.NoError(t, dq.schedule("4", "4", time.Millisecond*40))
		time.Sleep(time.Millisecond * 100)
		assertRcv(t, r, "1", "2", "3", "4")
		cancel()
	})
	t.Run("delay resolution is wrapped", func(t *testing.T) {
		dq := newQueue(10, 50*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		now := time.Now().Truncate(50 * time.Millisecond)
		require.NoError(t, dq.scheduleOnTime("1", "1", now.Add(time.Millisecond*75)))
		require.NoError(t, dq.scheduleOnTime("2", "2", now.Add(time.Millisecond*60)))
		time.Sleep(time.Millisecond * 200)
		assertRcv(t, r, "1", "2")

		cancel()
		assert.Equal(t, r.items[0].t.String(), r.items[1].t.String())
	})
	t.Run("delay resolution not wrapped", func(t *testing.T) {
		dq := newQueue(10, 50*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		now := time.Now().Truncate(50 * time.Millisecond)
		require.NoError(t, dq.scheduleOnTime("1", "1", now.Add(time.Millisecond*130)))
		require.NoError(t, dq.scheduleOnTime("2", "2", now.Add(time.Millisecond*80)))
		time.Sleep(time.Millisecond * 300)
		assertRcv(t, r, "2", "1")

		cancel()
		assert.NotEqual(t, r.items[0].t.String(), r.items[1].t.String())
	})

	t.Run("dedup same key", func(t *testing.T) {
		dq := newQueue(10, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("a", "a-first", time.Millisecond*20))
		require.NoError(t, dq.schedule("a", "a-second", time.Millisecond*20))
		require.NoError(t, dq.schedule("a", "a-third", time.Millisecond*20))
		require.NoError(t, dq.schedule("b", "b", time.Millisecond*20))

		time.Sleep(time.Millisecond * 100)
		assertRcv(t, r, "a-first", "b")

		// After firing, key "a" is released and can be scheduled again
		require.NoError(t, dq.schedule("a", "a-again", time.Millisecond*20))
		time.Sleep(time.Millisecond * 100)
		assertRcv(t, r, "a-first", "b", "a-again")
		cancel()
	})
	t.Run("dedup does not consume capacity", func(t *testing.T) {
		dq := newQueue(2, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("a", "a", time.Millisecond*20))
		require.NoError(t, dq.schedule("a", "a", time.Millisecond*20))
		require.NoError(t, dq.schedule("a", "a", time.Millisecond*20))
		// Capacity is 2, only one slot taken so this must still fit
		require.NoError(t, dq.schedule("b", "b", time.Millisecond*20))
		// Now full
		require.Error(t, dq.schedule("c", "c", time.Millisecond*20))
	})
	t.Run("if queue is full errpr", func(t *testing.T) {
		dq := newQueue(2, 10*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		r := &rcv{}
		go dq.run(ctx, r.OnItem)
		require.NoError(t, dq.schedule("1", "1", time.Millisecond*20))
		require.NoError(t, dq.schedule("2", "2", time.Millisecond*40))
		// Extra item can't be enqueued
		require.Error(t, dq.schedule("3", "3", time.Millisecond*40))
		time.Sleep(time.Millisecond * 100)
		assertRcv(t, r, "1", "2")

		require.NoError(t, dq.schedule("3", "3", time.Millisecond*20))
		require.NoError(t, dq.schedule("4", "4", time.Millisecond*40))
		time.Sleep(time.Millisecond * 100)
		assertRcv(t, r, "1", "2", "3", "4")
		cancel()
	})
}

func Test_dump(t *testing.T) {
	q := newQueue(3, 10*time.Millisecond)
	type testItem struct {
		id string
	}
	testItems := []testItem{
		{id: "1"},
		{id: "2"},
		{id: "3"},
		{id: "4"},
	}
	require.NoError(t, q.schedule(testItems[0].id, testItems[0], time.Millisecond*20))
	require.NoError(t, q.schedule(testItems[1].id, testItems[1], time.Millisecond*20))
	require.NoError(t, q.schedule(testItems[2].id, testItems[2], time.Millisecond*20))
	require.Error(t, q.schedule(testItems[3].id, testItems[3], time.Millisecond*20))

	dump := q.dump()
	require.ElementsMatch(t, testItems[:3], dump)
}
