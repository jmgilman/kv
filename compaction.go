package kv

// import (
// 	"container/heap"
// 	"errors"
// 	"io"
// )

// type CursorWrapper struct {
// 	current  KVPair
// 	cursor   Cursor
// 	priority int
// }

// func (c *CursorWrapper) Done() bool {
// 	return c.cursor.Done()
// }

// func (c *CursorWrapper) Next() error {
// 	next, err := c.cursor.Next()
// 	c.current = next
// 	return err
// }

// func NewCursorWrapper(cursor Cursor, priority int) CursorWrapper {
// 	return CursorWrapper{cursor: cursor, priority: priority}
// }

// type CursorWrapperHeap []CursorWrapper

// func (h CursorWrapperHeap) Len() int {
// 	return len(h)
// }

// func (h CursorWrapperHeap) Less(i, j int) bool {
// 	return h[i].current.Key < h[j].current.Key
// }

// func (h CursorWrapperHeap) Swap(i, j int) {
// 	h[i], h[j] = h[j], h[i]
// }

// func (h *CursorWrapperHeap) Push(x interface{}) {
// 	*h = append(*h, x.(CursorWrapper))
// }

// func (h *CursorWrapperHeap) Pop() interface{} {
// 	old := *h
// 	n := len(old)
// 	x := old[n-1]
// 	*h = old[0 : n-1]
// 	return x
// }

// func Compact(cursors []Cursor, writer SegmentWriter) error {
// 	// Wrap the passed in cursors to make them compatible with the heap
// 	h := CursorWrapperHeap{}
// 	for i, cursor := range cursors {
// 		// Order of cursors is highest priority (newest data) -> lowest priority
// 		wrapper := NewCursorWrapper(cursor, i)
// 		wrapper.Next() // Initialize the current field with the first pair
// 		heap.Push(&h, wrapper)
// 	}

// 	// Setup the last read pair
// 	c := heap.Pop(&h).(CursorWrapper)
// 	last := c.current
// 	lastPri := c.priority
// 	if err := processCursor(c, &h); err != nil {
// 		return err
// 	}

// 	// Iterate through the heap until it's empty
// 	for h.Len() > 0 {
// 		c := heap.Pop(&h).(CursorWrapper)

// 		// Check for duplicate keys
// 		if c.current.Key == last.Key {
// 			// Lower priority means the data is newer
// 			if c.priority < lastPri {
// 				last = c.current
// 				lastPri = c.priority
// 			}

// 			// Intentionally skip appending the current pair in case the next
// 			// pair is a duplicate as well
// 		} else {
// 			// Safe to append if the current pair is not a duplicate
// 			writer.Write(last)
// 			last = c.current
// 			lastPri = c.priority
// 		}

// 		// Push the cursor back to the heap
// 		if err := processCursor(c, &h); err != nil {
// 			return err
// 		}
// 	}

// 	writer.Write(last)
// 	return nil
// }

// func processCursor(c CursorWrapper, h *CursorWrapperHeap) error {
// 	if err := c.Next(); err != nil {
// 		if !errors.Is(err, io.EOF) {
// 			return err
// 		}
// 	}

// 	if !c.Done() {
// 		heap.Push(h, c)
// 	}

// 	return nil
// }
