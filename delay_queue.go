package kreconciler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type dq struct {
	head *qElt
	tail *qElt
	C    chan bool
	sync.Mutex
	resolution time.Duration
	size       int
	capacity   int
}

type qElt struct {
	next *qElt
	prev *qElt
	i    interface{}
	end  time.Time
}

func newQueue(size int, resolution time.Duration) *dq {
	head := &qElt{i: "head"}
	tail := &qElt{i: "tail"}
	head.next = tail
	tail.prev = head
	res := &dq{
		resolution: resolution,
		head:       head,
		tail:       tail,
		size:       0,
		capacity:   size,
		C:          make(chan bool, 1),
	}
	return res
}

func (q *dq) run(ctx context.Context, onItem func(t time.Time, i interface{})) {
	var timer = time.NewTimer(time.Hour)
	for {
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-q.C:
			timer.Stop()
			t := q.nextTime()
			if t.IsZero() {
				timer = time.NewTimer(time.Hour)
			} else {
				timer = time.NewTimer(t.Sub(time.Now()))
			}
		case now := <-timer.C:
			elts := q.dequeueIfBefore(now)
			for _, n := range elts {
				onItem(now, n)
			}
		}
	}
}

func (q *dq) schedule(i interface{}, delay time.Duration) error {
	return q.scheduleOnTime(i, time.Now().Add(delay))
}

func (q *dq) scheduleOnTime(i interface{}, delay time.Time) error {
	q.Lock()
	defer q.Unlock()
	if q.size == q.capacity {
		return fmt.Errorf("delay queue is full with %d items", q.capacity)
	}
	q.size += 1
	// Only do things in batch of 100 millis this avoids having 1000s of steps
	end := delay.Truncate(q.resolution)
	n := &qElt{
		i:   i,
		end: end,
	}
	cur := q.head.next
	for {
		if cur == q.tail || cur.end.After(end) {
			prev := cur.prev
			cur.prev = n
			prev.next = n
			n.next = cur
			n.prev = prev
			if prev == q.head {
				select {
				// If we replaced the head prepare to restart the tick
				case q.C <- true:
				default:
				}
			}
			return nil
		}
		cur = cur.next
	}
}

func (q *dq) dequeueIfBefore(t time.Time) []interface{} {
	q.Lock()
	defer q.Unlock()
	var res []interface{}
	for {
		n := q.head.next
		if n != q.tail && n.end.Before(t) {
			q.head.next = n.next
			n.next.prev = q.head
			res = append(res, n.i)
			n = n.next
		} else {
			if len(res) > 0 {
				q.size -= len(res)
				select {
				// If we replaced the head prepare to restart the tick
				case q.C <- true:
				default:
				}
			}
			return res
		}
	}
}

func (q *dq) nextTime() time.Time {
	q.Lock()
	defer q.Unlock()
	if q.head.next == q.tail {
		return time.Time{}
	}
	return q.head.next.end
}

func (d *dq) dump() []any {
	d.Lock()
	defer d.Unlock()
	var res []any
	cur := d.head.next
	for cur != d.tail {
		res = append(res, cur.i)
		cur = cur.next
	}
	return res
}
