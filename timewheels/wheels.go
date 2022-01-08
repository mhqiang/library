package timewheels

import (
	"fmt"
	"octopus/utils/cache"
	"octopus/utils/list"
	"time"
)

// Job 延时任务回调函数
type Job interface {
	RunTask(data interface{})
}

type timeItem struct {
	Position int
	ele      *list.Element
}

// TaskData 回调函数参数类型

// TimeWheel 时间轮
type TimeWheel struct {
	interval time.Duration // 指针每隔多久往前移动一格
	ticker   *time.Ticker
	slots    []*list.List // 时间轮槽
	// key: 定时器唯一标识 value: 定时器所在的槽, 主要用于删除定时器, 不会出现并发读写，不加锁直接访问

	// timer                map[interface{}]*timeItem
	timer                *cache.Cache
	currentPos           int                // 当前指针指向哪一个槽
	nexPos               int                // 下次要移动到的槽位
	slotNum              int                // 槽数量
	job                  map[string]Job     // 定时器回调函数, 根据类型去回调 多种任务类型
	addTaskChannel       chan *Task         // 新增任务channel
	addTaskPeriodChannel chan *list.Element // 已经加进来的任务
	removeTaskChannel    chan cache.Key     // 删除任务channel
	stopChannel          chan bool          // 停止定时器channel
}

// New 创建时间轮
func New(interval time.Duration, slotNum int, job map[string]Job) *TimeWheel {
	if interval <= 0 || slotNum <= 0 || job == nil {
		return nil
	}
	tw := &TimeWheel{
		interval: interval,
		slots:    make([]*list.List, slotNum),
		timer:    cache.NewCache(),
		// 初始化的时候定义好指定job 类型
		job:                  job,
		slotNum:              slotNum,
		addTaskChannel:       make(chan *Task, 100),
		addTaskPeriodChannel: make(chan *list.Element, 100),
		removeTaskChannel:    make(chan cache.Key),
		stopChannel:          make(chan bool),
	}

	tw.initSlots()

	return tw
}

// 初始化槽，每个槽指向一个双向链表
func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start 启动时间轮
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddTimer 添加定时器 key为定时器唯一标识
func (tw *TimeWheel) AddTimer(delay time.Duration, key cache.Key, data interface{},
	dataType string, period bool) {
	if delay < 0 {
		return
	}

	var t *Task
	item, exists := tw.timer.Get(key)
	if exists {
		curitem := item.(*timeItem)
		t = curitem.ele.Value.(*Task)

		t.key = key
		t.data = data
		t.taskType = dataType
		t.period = period
		t.periodTime = delay
		return
	}

	t = TaskGet()
	t.delay = delay
	t.periodTime = delay
	t.key = key
	t.data = data
	t.taskType = dataType
	t.period = period

	tw.addTaskChannel <- t

}

// AddTimer 添加定时器 key为定时器唯一标识
func (tw *TimeWheel) AddTimerRobin(delay time.Duration, key cache.Key, data interface{},
	dataType string, period bool, index int) {
	if delay < 0 {
		return
	}

	delayTime := time.Duration(index%tw.slotNum) * tw.interval

	var t *Task
	item, exists := tw.timer.Get(key)
	if exists {
		curitem := item.(*timeItem)
		t = curitem.ele.Value.(*Task)
		// t.delay = delayTime
		t.key = key
		t.data = data
		t.taskType = dataType
		t.period = period
		t.periodTime = delay
		return
	}

	t = TaskGet()
	t.delay = delayTime
	t.key = key
	t.data = data
	t.taskType = dataType
	t.period = period
	t.periodTime = delay

	tw.addTaskChannel <- t

}

// RemoveTimer 删除定时器 key为添加定时器时传递的定时器唯一标识
func (tw *TimeWheel) RemoveTimer(key cache.Key) {
	if key == nil {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.currentPos = tw.nexPos
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(task)
		case ele := <-tw.addTaskPeriodChannel:
			tw.addTaskPeriod(ele)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	tw.scanAndRunTask(l)
	if tw.currentPos == tw.slotNum-1 {
		tw.nexPos = 0
	} else {
		tw.nexPos++
	}
}

// 扫描链表中过期定时器, 并执行回调函数
func (tw *TimeWheel) scanAndRunTask(l *list.List) {

	for e := l.Front(); e != nil; {
		task := e.Value.(*Task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		//  注意堵塞解决
		if job, exists := tw.job[task.taskType]; exists {
			go job.RunTask(task.data)
		}

		next := e.Next()
		l.Remove(e)

		// 周期任务操作,
		if task.period {
			tw.addTaskPeriod(e)
		} else {
			if task.key != nil {
				tw.timer.Delete(task.key)
				TaskPut(task)
			}
		}

		e = next
	}
}

// 新增任务到链表中
func (tw *TimeWheel) addTask(task *Task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle
	fmt.Println(task.circle)
	ele := tw.slots[pos].PushBack(task)

	if task.key != nil {
		item := &timeItem{
			Position: pos,
			ele:      ele,
		}
		tw.timer.Set(task.key, item)
	}
}

// 不修改任务，push back to指定链表
func (tw *TimeWheel) addTaskPeriod(ele *list.Element) {
	task := ele.Value.(*Task)

	pos, circle := tw.getPositionAndCircle(task.periodTime)
	task.circle = circle
	fmt.Println("task.circle", task.circle)
	tw.slots[pos].PushBackElement(ele)

	if task.key != nil {
		item, exists := tw.timer.Get(task.key)
		if exists {
			curItem := item.(*timeItem)
			curItem.Position = pos

		}

	}
}

// 获取定时器在槽中的位置, 时间轮需要转动的圈数
func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)

	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum
	// log.Println(tw.currentPos, delaySeconds, intervalSeconds, tw.slotNum, pos, circle)
	return
}

// 从链表中删除任务
func (tw *TimeWheel) removeTask(key cache.Key) {
	// 获取定时器所在的槽
	item, ok := tw.timer.Get(key)
	if !ok {
		return
	}

	curItem := item.(*timeItem)
	// 获取槽指向的链表
	l := tw.slots[curItem.Position]
	l.Remove(curItem.ele)

	tw.timer.Delete(key)

	if curItem.ele.Value != nil {
		TaskPut(curItem.ele.Value.(*Task))
	}

}
