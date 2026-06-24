package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalOpen
)

type CircuitBreaker struct {
	mu    sync.Mutex
	state State

	failures          int
	failuresThreshold int

	openTimeout time.Duration
	openedAt    time.Time

	halOpenSuccesses int
	successThreshold int
}

func NewCircuitBreaker(failureThreshold int, openTimeout time.Duration, successThreshold int) *CircuitBreaker {
	return &CircuitBreaker{
		state:             StateClosed,
		failures:          0,
		failuresThreshold: failureThreshold,
		openTimeout:       openTimeout,
		openedAt:          time.Now(),
		halOpenSuccesses:  0,
		successThreshold:  successThreshold,
	}
}

var ErrorOpen = errors.New("circuit breaker is open")

func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.allow() {
		return ErrorOpen
	}

	err := fn()
	cb.record(err)

	return err
}

func (cb *CircuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.openedAt) >= cb.openTimeout {
			cb.state = StateHalOpen
			cb.halOpenSuccesses = 0
			return true
		}
		return false
	case StateHalOpen:
		return true
	}

	return false
}

func (cb *CircuitBreaker) record(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		switch cb.state {
		case StateClosed:
			cb.failures++
			if cb.failures >= cb.failuresThreshold {
				cb.trip()
			}
		case StateHalOpen:
			cb.trip()
		}
		return
	}

	switch cb.state {
	case StateClosed:
		cb.failures = 0
	case StateHalOpen:
		cb.halOpenSuccesses++
		if cb.halOpenSuccesses >= cb.successThreshold {
			cb.reset()
		}
	}

}

func (cb *CircuitBreaker) trip() {
	cb.state = StateOpen
	cb.openedAt = time.Now()
	cb.failures = 0
}

func (cb *CircuitBreaker) reset() {
	cb.state = StateClosed
	cb.failures = 0
}
