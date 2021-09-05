package channelchainer

import "sync"

type atomicError struct {
	m   sync.Mutex
	err error
}

func newAtomicError(err error) *atomicError {
	return &atomicError{err: err}
}

func (a *atomicError) Soft(err error) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.err == nil {
		a.err = err
	}
}

func (a *atomicError) Get() error {
	a.m.Lock()
	defer a.m.Unlock()
	return a.err
}
