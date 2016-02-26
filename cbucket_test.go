package container_test

import (
	"fmt"
	"sync"
	"time"

	. "github.com/antlinker/container"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("按数量时间弹出桶的容器功能测试", func() {
	var (
		buckets_slicenum = 100
		buckets_popnum   = 100
		buckets_timeout  = 1 * time.Second
	)
	It("插入元素及数量测试", func() {
		insertnum := 100
		buckets := CreateNTCBuckets(buckets_popnum, buckets_timeout, buckets_slicenum)
		buckets.Open()
		for i := 0; i < insertnum; i++ {
			buckets.Push(i)
		}
		By(fmt.Sprintf("插入元素数量：%d", buckets.Len()))
		Ω(buckets.Len()).Should(Equal(insertnum))
	})

	It("同步插入弹出", func() {
		insertnum := int64(10000)
		buckets := CreateNTCBuckets(buckets_popnum, buckets_timeout, buckets_slicenum)
		bucketchan, _ := buckets.Open()
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func(bucketchan <-chan CBucket) {
			//			defer GinkgoRecover()
			defer wg.Done()

			var sum int64 = 0
			for bucket := range bucketchan {
				if bucket == nil {
					break
				}
				//elems, _ := bucket.ToSlice()
				//Ω(bucket.Len()).Should(Equal(len(elems)))
				for bucket.Len() > 0 {
					elem, err := bucket.Pop()
					if err != nil {
						break
					}
					//Ω(elem).Should(BeNumerically("<", insertnum))
					sum += elem.(int64)
				}

			}
			Ω(sum).Should(Equal(insertnum * (insertnum - 1) / 2))
		}(bucketchan)

		for i := int64(0); i < insertnum; i++ {
			buckets.Push(i)
		}
		buckets.Close()
		wg.Wait()
		Ω(buckets.Len()).Should(Equal(0))
	})
	It("同步插入弹出，转换切片", func() {
		insertnum := int64(10000)
		buckets := CreateNTCBuckets(buckets_popnum, buckets_timeout, buckets_slicenum)
		bucketchan, _ := buckets.Open()
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func(bucketchan <-chan CBucket) {
			//			defer GinkgoRecover()
			defer wg.Done()

			var sum int64 = 0
			for bucket := range bucketchan {
				if bucket == nil {
					break
				}
				elems, _ := bucket.ToSlice()
				Ω(bucket.Len()).Should(Equal(len(elems)))
				for _, elem := range elems {

					//Ω(elem).Should(BeNumerically("<", insertnum))
					sum += elem.(int64)
				}

			}
			Ω(sum).Should(Equal(insertnum * (insertnum - 1) / 2))
		}(bucketchan)

		for i := int64(0); i < insertnum; i++ {
			buckets.Push(i)
		}
		buckets.Close()
		wg.Wait()
		Ω(buckets.Len()).Should(Equal(0))
	})
	It("异步插入弹出", func() {
		gonum := 100
		insertnum := int64(10000)
		buckets := CreateNTCBuckets(buckets_popnum, buckets_timeout, buckets_slicenum)
		bucketchan, _ := buckets.Open()
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func(bucketchan <-chan CBucket) {
			//			defer GinkgoRecover()
			defer wg.Done()

			var sum int64 = 0
			for bucket := range bucketchan {
				if bucket == nil {
					break
				}
				//fmt.Println("弹出桶:", bucket.Len())
				for bucket.Len() > 0 {

					elem, err := bucket.Pop()
					if err != nil {
						break
					}
					//Ω(elem).Should(BeNumerically("<", insertnum))
					sum += elem.(int64)
				}

			}
			Ω(sum).Should(Equal(100 * insertnum * (insertnum - 1) / 2))
		}(bucketchan)
		addwg := &sync.WaitGroup{}
		for n := 0; n < gonum; n++ {
			addwg.Add(1)
			go func() {
				defer addwg.Done()
				for i := int64(0); i < insertnum; i++ {
					buckets.Push(i)
				}
			}()
		}
		addwg.Wait()
		buckets.Close()

		wg.Wait()
		Ω(buckets.Len()).Should(Equal(0))
	})

})
var _ = Describe("按数量时间弹出桶的容器功能测试", func() {
	var (
		buckets_slicenum = 100
		buckets_popnum   = 100
		buckets_timeout  = 1 * time.Second
		buckets          CBuckets
		bucketchan       <-chan CBucket
		wg               *sync.WaitGroup
		starttime        time.Time
		insertnum        int64 = 10000
		gonum            int64 = 100
	)
	BeforeSuite(func() {
		buckets = CreateNTCBuckets(buckets_popnum, buckets_timeout, buckets_slicenum)
		bucketchan, _ = buckets.Open()
		wg = &sync.WaitGroup{}
		wg.Add(1)
		go func(bucketchan <-chan CBucket) {
			//			defer GinkgoRecover()
			defer wg.Done()

			var sum int64 = 0
			for bucket := range bucketchan {
				if bucket == nil {
					break
				}
				//fmt.Println("弹出桶:", bucket.Len())
				for bucket.Len() > 0 {

					elem, err := bucket.Pop()
					if err != nil {
						break
					}
					//Ω(elem).Should(BeNumerically("<", insertnum))
					sum += elem.(int64)
				}

			}
			Ω(sum).Should(Equal(100 * insertnum * (insertnum - 1) / 2))
		}(bucketchan)
		starttime = time.Now()
	})
	AfterSuite(func() {
		buckets.Close()

		wg.Wait()
		endtime := time.Now()
		stime := endtime.Sub(starttime)
		fmt.Println("测试时长：", stime)
		fmt.Println("插入数据量：", insertnum*gonum)
		fmt.Println("平均用时：", int64(stime)/(insertnum*gonum))
	})
	Measure("基准测试插入测试", func(b Benchmarker) {

		runtime := b.Time("runtime", func() {
			for i := int64(0); i < insertnum; i++ {
				buckets.Push(i)
			}

		})
		Ω(runtime.Seconds()).Should(BeNumerically("<", 0.4), "SomethingHard() shouldn't take too long.")

	}, int(gonum))
})
