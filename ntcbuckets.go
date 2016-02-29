package container

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func CreateNTCBuckets(num int, timeout time.Duration, slicenum int) *NTCBuckets {
	bs := &NTCBuckets{popnum: num, timeout: timeout, curCBucket: createNTCBucket(slicenum)}
	return bs
}

type NTCBuckets struct {
	bchan      chan CBucket
	run        bool
	closelock  sync.Mutex
	size       int64
	curCBucket *NTCBucket
	poplock    sync.RWMutex

	popnum  int
	timeout time.Duration

	pushSign chan bool

	pushCond *sync.Cond
}

var CloseError error = errors.New("已经关闭")
var emptyError error = errors.New("已经为空")

func (b *NTCBuckets) popBucket() {
	//fmt.Println("弹出桶:start")
	b.poplock.Lock()
	b.size = 0
	popbucket := b.curCBucket
	b.curCBucket = createNTCBucket(popbucket.slicenum)

	b.poplock.Unlock()
	//fmt.Println("弹出桶:", popbucket)
	b.bchan <- popbucket
	//fmt.Println("弹出桶完成:", popbucket)
}
func (b *NTCBuckets) popCheck() bool {
	return int(b.size) >= b.popnum
}
func (b *NTCBuckets) Close() {
	b.closelock.Lock()
	defer b.closelock.Unlock()
	if !b.run {
		return
	}
	b.run = false
	b.pushCond.Signal()
}
func (b *NTCBuckets) Open() (<-chan CBucket, error) {
	b.curCBucket.reset()
	b.bchan = make(chan CBucket)
	b.pushSign = make(chan bool, 16)
	b.pushCond = sync.NewCond(&sync.Mutex{})
	go func(pushSign chan bool) {
		for b.run {
			select {
			case <-time.After(b.timeout):
				if b.size > 0 {
					b.popBucket()
				}
			case r := <-pushSign:
				if !r {
					break
				}
				for b.popCheck() {
					b.popBucket()
					runtime.Gosched()
				}

			}

		}
		if b.size > 0 {
			b.popBucket()
		}
		close(b.bchan)
	}(b.pushSign)
	go func(pushCond *sync.Cond, pushSign chan bool) {
		pushCond.L.Lock()
		defer pushCond.L.Unlock()
		for b.run {
			//fmt.Println("弹出桶信号发出")
			pushSign <- true
			pushCond.Wait()

		}
		pushSign <- false
		close(b.pushSign)
	}(b.pushCond, b.pushSign)
	b.run = true
	return b.bchan, nil
}
func (b *NTCBuckets) Push(ele interface{}) error {
	b.closelock.Lock()
	if !b.run {
		return CloseError
	}
	b.poplock.RLock()
	b.curCBucket.push(ele)
	atomic.AddInt64(&b.size, 1)

	//fmt.Println("发出信号------")
	b.pushCond.Signal()
	b.poplock.RUnlock()
	b.closelock.Unlock()
	return nil
}

func (b *NTCBuckets) Len() int {
	return int(b.size)
}

func createNTCBucket(slicenum int) *NTCBucket {
	b := new(NTCBucket)
	b.init(slicenum)
	return b
}

type NTCBucket struct {
	size     int
	slicenum int
	blists   []*CBucketList
	popind   int
	poplock  sync.Mutex
}

func (b *NTCBucket) init(slicenum int) {
	b.slicenum = slicenum
	b.blists = make([]*CBucketList, slicenum)
	for i := 0; i < slicenum; i++ {
		b.blists[i] = createCBucketList()
	}
}
func (b *NTCBucket) reset() {
	var slicenum = b.slicenum
	for i := 0; i < slicenum; i++ {
		b.blists[i] = createCBucketList()
	}
}
func (b *NTCBucket) push(ele interface{}) error {
	b.size++
	var wslice = (b.size - 1) % b.slicenum
	b.blists[wslice].push(ele)
	return nil
}
func (b *NTCBucket) Pop() (interface{}, error) {
	b.poplock.Lock()
	defer b.poplock.Unlock()
	if b.size == 0 {
		return nil, emptyError
	}
	b.size--
	ele := b.blists[b.popind].pop()
	if b.popind >= b.slicenum-1 {
		b.popind = 0
	} else {
		b.popind++
	}
	// if !atomic.CompareAndSwapInt32(&b.popind, int32(b.slicenum-1), 0) {
	// 	b.popind++
	// }

	// if b.blists[0].isempty() && len(b.blists) > 1 {
	// 	b.blists = b.blists[1:]
	// }
	return ele, nil
}
func (b *NTCBucket) Len() int {
	return b.size
}

func (b *NTCBucket) ToSlice() ([]interface{}, error) {
	var slicenum = b.slicenum

	out := make([]interface{}, 0, b.size)

	for {
		for i := 0; i < slicenum; i++ {
			elem := b.blists[i].pop()
			if elem == nil {
				return out, nil
			}
			out = append(out, elem)
		}
	}
}

type CBucketNode struct {
	value interface{}
	next  *CBucketNode
}

func (n *CBucketNode) insertNext(ele interface{}) *CBucketNode {
	node := new(CBucketNode)
	node.value = ele
	n.next = node
	return node
}

func createCBucketList() *CBucketList {
	list := new(CBucketList)
	list.cur = &list.root
	return list
}

type CBucketList struct {
	root CBucketNode

	cur *CBucketNode

	sync.RWMutex
}

func (l *CBucketList) isempty() bool {
	return l.root.next == nil
}
func (l *CBucketList) clear() {
	l.Lock()
	l.cur = &l.root
	l.root.next = nil
	l.Unlock()
}
func (l *CBucketList) join(list *CBucketList) {
	l.Lock()
	l.cur.next = list.root.next
	l.cur = list.cur
	l.Unlock()
}
func (l *CBucketList) push(ele interface{}) {
	l.Lock()
	l.cur = l.cur.insertNext(ele)
	l.Unlock()
}
func (l *CBucketList) pop() (ele interface{}) {
	l.Lock()
	//l.cur.
	if l.root.next == nil {
		l.Unlock()
		return nil
	}
	node := l.root.next
	l.root.next = node.next
	ele = node.value
	node.next = nil
	l.Unlock()
	return
}
