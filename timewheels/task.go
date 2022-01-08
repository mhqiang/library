package timewheels

import (
	"octopus/utils/cache"
	"sync"
	"time"
)

// Task 延时任务
type Task struct {
	delay      time.Duration // 延迟时间
	circle     int           // 时间轮需要转动几圈
	key        cache.Key     // 定时器唯一标识, 用于删除定时器
	data       interface{}   // 回调函数参数
	taskType   string        // 对应job key
	period     bool          // 是否周期任务
	periodTime time.Duration // 调度周期
}

var taskPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return new(Task)
	},
}

func (t *Task) Reset() {
	t.key = nil
	t.data = nil
	t.period = false
}

func TaskGet() *Task {
	return taskPool.Get().(*Task)
}

func TaskPut(t *Task) {
	if t == nil {
		return
	}
	t.Reset()
	taskPool.Put(t)
}
