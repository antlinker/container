package container

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CreateNTCBuckets 创建一个桶管理容器
// 当容器内有num个数据或则到达timeout超时时间容器内数据不为空则弹出一个桶（桶内包含多个元素）
// slicenum内部桶数量
func CreateNTCBuckets(num int, timeout time.Duration, slicenum int) CBuckets {
	bs := &ntcBuckets{popnum: num, timeout: timeout, curCBucket: createNTCBucket(slicenum)}
	return bs
}

type ntcBuckets struct {
	bchan      chan CBucket
	run        bool
	closelock  sync.Mutex
	size       int64
	curCBucket *ntcBucket
	poplock    sync.RWMutex

	popnum  int
	timeout time.Duration

	pushSign chan bool

	pushCond *sync.Cond
}

// ErrClose 容器已经关闭
var ErrClose = errors.New("已经关闭")

var errEmpty = errors.New("已经为空")

func (b *ntcBuckets) popBucket() {
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
func (b *ntcBuckets) popCheck() bool {
	return int(b.size) >= b.popnum
}
func (b *ntcBuckets) Close() {
	b.closelock.Lock()
	defer b.closelock.Unlock()
	if !b.run {
		return
	}
	b.run = false
	b.pushCond.Signal()
}
func (b *ntcBuckets) Open() (<-chan CBucket, error) {
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
func (b *ntcBuckets) Push(ele interface{}) error {
	b.closelock.Lock()
	if !b.run {
		return ErrClose
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

func (b *ntcBuckets) Len() int {
	return int(b.size)
}

func createNTCBucket(slicenum int) *ntcBucket {
	b := new(ntcBucket)
	b.init(slicenum)
	return b
}

type ntcBucket struct {
	size     int
	slicenum int
	blists   []*cBucketList
	popind   int
	poplock  sync.Mutex
}

func (b *ntcBucket) init(slicenum int) {
	b.slicenum = slicenum
	b.blists = make([]*cBucketList, slicenum)
	for i := 0; i < slicenum; i++ {
		b.blists[i] = createCBucketList()
	}
}
func (b *ntcBucket) reset() {
	var slicenum = b.slicenum
	for i := 0; i < slicenum; i++ {
		b.blists[i] = createCBucketList()
	}
}
func (b *ntcBucket) push(ele interface{}) error {
	b.size++
	var wslice = (b.size - 1) % b.slicenum
	b.blists[wslice].push(ele)
	return nil
}
func (b *ntcBucket) Pop() (interface{}, error) {
	b.poplock.Lock()
	defer b.poplock.Unlock()
	if b.size == 0 {
		return nil, errEmpty
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
func (b *ntcBucket) Len() int {
	return b.size
}

func (b *ntcBucket) ToSlice() ([]interface{}, error) {
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

type cBucketNode struct {
	value interface{}
	next  *cBucketNode
}

func (n *cBucketNode) insertNext(ele interface{}) *cBucketNode {
	node := new(cBucketNode)
	node.value = ele
	n.next = node
	return node
}

func createCBucketList() *cBucketList {
	list := new(cBucketList)
	list.cur = &list.root
	return list
}

type cBucketList struct {
	root cBucketNode

	cur *cBucketNode

	sync.RWMutex
}

func (l *cBucketList) isempty() bool {
	return l.root.next == nil
}
func (l *cBucketList) clear() {
	l.Lock()
	l.cur = &l.root
	l.root.next = nil
	l.Unlock()
}
func (l *cBucketList) join(list *cBucketList) {
	l.Lock()
	l.cur.next = list.root.next
	l.cur = list.cur
	l.Unlock()
}
func (l *cBucketList) push(ele interface{}) {
	l.Lock()
	l.cur = l.cur.insertNext(ele)
	l.Unlock()
}
func (l *cBucketList) pop() (ele interface{}) {
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
