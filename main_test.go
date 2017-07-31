package main

import (
	"testing"
	"fmt"
	"errors"
	"time"
	"os"
)

type MockKandi struct {
	started			int
	errorsToReturn		int
}

func (m *MockKandi) Start(conf *Config) (error) {
	if m.started < m.errorsToReturn {
		m.started = m.started + 1
		return errors.New("SomeError")
	}
	return nil
}

var tests = []struct {
	label				string
	errorsToReturn			int
	backoffMax			int
	backoffTime			int
	backoffResetTime		int
	verify 				func(label string, taken time.Duration, t *testing.T)
}{
	{
		"Should restart when application exits with error",
		5,
		-1,
		0,
		-1,
		func(label string, taken time.Duration, t *testing.T) {
			if taken > 1 * time.Second {
				t.Error(fmt.Sprintf("%s: should not have waited more than %s, but waited %s", label, 3 * time.Second, taken))
			}
		},
	},
	{
		"Should backoff at a fixed time rate",
		2,
		0,
		1,
		-1,
		func(label string, taken time.Duration, t *testing.T) {
			if taken < 3 * time.Second || taken > 4 * time.Second {
				t.Error(fmt.Sprintf("%s: should have waited longer than %s, but waited %s", label, 3 * time.Second, taken))
			}
		},
	},
	{
		"Should be limited to a max backoff time",
		2,
		1,
		1,
		-1,
		func(label string, taken time.Duration, t *testing.T) {
			if taken < 2 * time.Second || taken > 3 * time.Second {
				t.Error(fmt.Sprintf("%s: should have waited longer than %s, but waited %s", label, 2 * time.Second, taken))
			}
		},
	},
	{
		"Should reset backoff",
		5,
		2,
		1,
		1,
		func(label string, taken time.Duration, t *testing.T) {
			if taken < 5 * time.Second || taken > 6 * time.Second {
				t.Error(fmt.Sprintf("%s: should have waited longer than %s, but waited %s", label, 5 * time.Second, taken))
			}
		},
	},
}

func Test(t *testing.T) {
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T){
			kandi := MockKandi{0, test.errorsToReturn}
			startTime := time.Now()
			kandiConf := KandiConfig{Backoff: &Backoff{test.backoffMax, test.backoffTime, test.backoffResetTime}}
			start(&Config{Kandi:&kandiConf}, &kandi)
			if kandi.started != kandi.errorsToReturn {
				t.Error(fmt.Sprintf("%s: kandi did not retry every error. expected: %d, actual: %d", test.label, kandi.started, kandi.errorsToReturn))
			}
			test.verify(test.label, time.Since(startTime), t)
		})
	}
}

func Test_Integration(t *testing.T) {
	os.Setenv("KANDI_CONFIG.PATH", "./etc/dev/integration.yaml")
	main()
}

func Test_main_should_stop_with_no_error(t *testing.T) {
	start(&Config{Kandi:&KandiConfig{Backoff: &Backoff{1, 1, 1}}}, &MockKandi{0, 0})
}

func Test_main_should_retry_when_reporting_errors(t *testing.T) {
	label := "retry when reporting errors"
	kandi := &MockKandi{0, 3}
	kandiConf := KandiConfig{Backoff: &Backoff{5, 1, 1}}
	start(&Config{Kandi:&kandiConf}, kandi)
	if kandi.started != kandi.errorsToReturn {
		t.Error(fmt.Sprintf("%s: kandi did not retry every error. expected: %d, actual: %d", label, kandi.started, kandi.errorsToReturn))
	}
}