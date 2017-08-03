package main

type MockBackoff struct {
	backoffCounter int
}

func NewMockBackoff() *MockBackoff {
	return &MockBackoff{}
}

func (b *MockBackoff) Backoff() {
	b.backoffCounter++
}
