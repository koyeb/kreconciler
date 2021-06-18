package kreconciler

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type operation struct {
	insert        bool
	id            string
	expectedError error
}

func TestObjectLocks(t *testing.T) {
	testCases := map[string]struct {
		inserts  []operation
		capacity int
		endMap   map[string]bool
	}{
		"insertsThenFree": {
			inserts: []operation{
				{insert: true, id: "a"},
				{insert: true, id: "b"},
				{insert: true, id: "c"},
				{insert: true, id: "d"},
				{insert: false, id: "a"},
				{insert: false, id: "b"},
				{insert: false, id: "c"},
				{insert: false, id: "d"},
			},
			capacity: 4,
			endMap:   map[string]bool{},
		},
		"insertsMultiNoDupe": {
			inserts: []operation{
				{insert: true, id: "a"},
				{insert: true, id: "a", expectedError: errAlreadyPresent},
				{insert: true, id: "b"},
				{insert: true, id: "b", expectedError: errAlreadyPresent},
			},
			capacity: 4,
			endMap:   map[string]bool{"a": true, "b": true},
		},
		"insertsAtCapacityOverflow": {
			inserts: []operation{
				{insert: true, id: "a"},
				{insert: true, id: "b"},
				{insert: true, id: "c", expectedError: errQueueOverflow},
			},
			capacity: 2,
			endMap:   map[string]bool{"a": true, "b": true},
		},
		"insertsAtCapacityNoOverflowAfterARemove": {
			inserts: []operation{
				{insert: true, id: "a"},
				{insert: true, id: "b"},
				{insert: true, id: "c", expectedError: errQueueOverflow},
				{insert: false, id: "b"},
				{insert: true, id: "c"},
			},
			capacity: 2,
			endMap:   map[string]bool{"a": true, "c": true},
		},
	}

	for n, tt := range testCases {
		t.Run(n, func(t *testing.T) {
			locks := newObjectLocks(tt.capacity)

			for _, v := range tt.inserts {
				if v.insert {
					err := locks.Take(v.id)
					require.Equal(t, v.expectedError, err)
				} else {
					locks.Free(v.id)
				}
			}
			assert.Equal(t, tt.endMap, locks.objects)
		})
	}
}
