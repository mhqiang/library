package timewheels

import (
	"log"
	"testing"
	"time"
)

type curJob struct{}

var tw *TimeWheel

func (j *curJob) RunTask(data interface{}) {

	log.Println("==========", tw.currentPos)

	// tw.AddTimer(4*time.Second, "test", "hello", "test", true)
}

type key struct {
	key string
}

func (k *key) ToBytes() []byte {
	return []byte(k.key)
}

func (k *key) ToString() string {
	return k.key
}

func TestAddTimer(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	jobs := make(map[string]Job)
	jobs["test"] = &curJob{}
	tw = New(2*time.Second, 30, jobs)
	tw.Start()
	log.Println("==========")
	k := &key{
		key: "test",
	}
	tw.AddTimerRobin(4*time.Second, k, "hello", "test", true, 0)
	select {}
}
