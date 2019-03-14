package leader

import (
	"sync"
	"time"

	"github.com/gofrs/uuid"
)

// Configs
var (
	HeartBeat               = 15 * time.Second
	Timeout                 = 25 * time.Second
	Op            Operation = noop{}
	OnLeadChanged           = func(lead bool) {}
)

// Operation type
type Operation interface {
	Begin() error
	Commit() error
	Rollback() error

	Current() (processID string, lastHeartBeat, now time.Time, err error)
	TryAcquire(processID string) error
}

var (
	processID = uuid.Must(uuid.NewV4()).String()
	mu        sync.RWMutex
	lead      bool
)

// ProcessID returns process id
func ProcessID() string {
	return processID
}

// Lead returns true if current process is the leader
func Lead() bool {
	mu.RLock()
	l := lead
	mu.RUnlock()
	return l
}

// Run runs f if current process is the leader
func Run(f func()) bool {
	l := Lead()
	if l {
		f()
	}
	return l
}

// Acquire starts acquire loop (must run in go routine)
func Acquire() {
	for {
		tryAcquire()
		time.Sleep(HeartBeat)
	}
}

func tryAcquire() {
	mu.Lock()
	defer mu.Unlock()

	isLead := lead
	lead = false

	defer func() {
		if isLead != lead {
			go OnLeadChanged(lead)
		}
	}()

	err := Op.Begin()
	if err != nil {
		return
	}
	defer Op.Rollback()

	acquire := func() {
		err := Op.TryAcquire(processID)
		if err != nil {
			return
		}
		err = Op.Commit()
		if err != nil {
			return
		}

		lead = true
	}

	current, lastBeat, now, err := Op.Current()
	if err != nil {
		return
	}

	// no leader
	if current == "" {
		acquire()
		return
	}

	// already be leader
	if current == processID {
		acquire()
		return
	}

	// not leader, check is current leader timeout
	if now.After(lastBeat.Add(Timeout)) {
		acquire()
		return
	}
}

type noop struct{}

func (noop) Begin() error {
	return nil
}

func (noop) Rollback() error {
	return nil
}

func (noop) Commit() error {
	return nil
}

func (noop) Current() (processID string, lastHeartBeat, now time.Time, err error) {
	return processID, time.Now(), time.Now(), nil
}

func (noop) TryAcquire(processID string) error {
	return nil
}
