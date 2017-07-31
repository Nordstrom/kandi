package main

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"errors"
	"testing"
	"fmt"
	"time"
)

type MockClient struct {
	conf		*InfluxConfig
	sent		[]influx.BatchPoints
}

func (c *MockClient) Ping(t time.Duration) (time.Duration, string, error) {
	return time.Duration(5), "", nil
}

func (c *MockClient) Write(batch influx.BatchPoints) (error) {
	if batch == nil {
		return errors.New("Batch must not be nil")
	}
	if c.sent != nil {
		c.sent = append(c.sent, batch)
	} else {
		c.sent = []influx.BatchPoints{batch}
	}
	return nil
}

func (c *MockClient) Query(query influx.Query) (*influx.Response, error) {
	return nil, nil
}

func NewPoints(name string, value interface{}) *influx.Point {
	tags := make(map[string]string)
	tags["env"] = "test"
	fields := make(map[string]interface{})
	fields["value"] = value
	pt, _ := influx.NewPoint(name, tags, fields, time.Now())
	return pt
}

var Influx_Client_Integration_Valid_Test_Cases =[]struct {
	label			string
	expectedError		string
	points 			[]*influx.Point
	conf 			*InfluxConfig
}{
	{
		"When Writing 1 Valid Point",
		"",
		[]*influx.Point{NewPoints("writingValidPoint", 1)},
		&InfluxConfig{
			"http://influx:8086",
			"user",
			"pass",
			10 * time.Second,
			"kandi-test",
			"test",
			"",
			1,
			"any",
			"autogen",
		},
	},
	{
		"When Writing 1 Valid Point With Conflicting Type",
		"{\"error\":\"write failed: field type conflict: input field \\\"value\\\" on measurement \\\"writingValidPoint\\\" is type string, already exists as type integer dropped=1\"}\n",
		[]*influx.Point{NewPoints("writingValidPoint", "1")},
		&InfluxConfig{
			"http://influx:8086",
			"user",
			"pass",
			10 * time.Second,
			"kandi-test",
			"test",
			"",
			1,
			"any",
			"autogen",
		},
	},
}

var Influx_Client_Integration_Query_Test_Cases = []struct {
	label         string
	config        *InfluxConfig
	expectedError string
}{
	{
		"When Database Is Not Present",
		&InfluxConfig{
			"http://influx:8086",
			"user",
			"pass",
			10 * time.Second,
			"kandi-test",
			"test",
			"",
			1,
			"any",
			"autogen",
		},
		"",
	},
	{
		"When Database Is Not Present",
		&InfluxConfig{
			"http://influx:8086",
			"user",
			"pass",
			10 * time.Second,
			"kandi-test",
			"fake",
			"",
			1,
			"any",
			"autogen",
		},
		"error authorizing query: user not authorized to execute statement 'SHOW SERIES'",
	},
	{
		"When Password Is Not Authorized",
		&InfluxConfig{
			"http://influx:8086",
			"user",
			"fake",
			10 * time.Second,
			"kandi-test",
			"test",
			"",
			1,
			"any",
			"autogen",
		},
		"authorization failed",
	},
	{
		"When User Is Not Authorized",
		&InfluxConfig{
			"http://influx:8086",
			"fake",
			"pass",
			10 * time.Second,
			"kandi-test",
			"test",
			"",
			1,
			"any",
			"autogen",
		},
		"authorization failed",
	},
}

func Test_Integration_Writing_Valid_Batch_To_Influx(t *testing.T) {
	if testing.Verbose() {
		for _, test := range Influx_Client_Integration_Valid_Test_Cases {
			t.Run(test.label, func(t *testing.T){
				sut, err := NewInfluxClient(test.conf)
				if err != nil {
					t.Fatal("failed to create client")
				}
				bpConf := influx.BatchPointsConfig{Database:test.conf.Database}
				bp, err := influx.NewBatchPoints(bpConf)

				for _, point := range test.points {
					bp.AddPoint(point)
				}
				err = sut.Write(bp)
				if err == nil && test.expectedError != "" {
					t.Fatal(fmt.Sprintf("%s: '''%s''' was expected but found no error", test.label, test.expectedError))
				} else if err !=nil && err.Error() != test.expectedError {
					t.Fatal(fmt.Sprintf("%s: '''%s''' was expected but found '''%s'''", test.label, test.expectedError, err.Error()))
				}
			})
		}
	}
}

func Test_Integration_Ping(t *testing.T) {
	if testing.Verbose() {
		for _, test := range Influx_Client_Integration_Valid_Test_Cases {
			t.Run(test.label, func(t *testing.T) {
				sut, err := NewInfluxClient(test.conf)
				if err != nil {
					t.Error(fmt.Sprintf("%s: Unable create influx client to ping influx and received error: %s", test.label, err.Error()))
				}
				time, _, err := sut.Ping(time.Minute)
				if err != nil {
					t.Error(fmt.Sprintf("%s: Unable to ping influx and received error: %s. time: %s", test.label, err.Error(), time))
				}
			})
		}
	}
}

func Test_Integration_Database_Connections(t *testing.T) {
	if testing.Verbose() {
		for _, test := range Influx_Client_Integration_Query_Test_Cases {
			t.Run(test.label, func(t *testing.T) {
				sut, err := NewInfluxClient(test.config)
				if err != nil {
					t.Fatal("failed to create client")
				}
				query := influx.NewQuery("SHOW SERIES",test.config.Database,test.config.Precision)
				actual, _ := sut.Query(query)
				if actual.Err != test.expectedError {
					t.Fatal(fmt.Sprintf("%s: '%s' was expected but found '%s'", test.label, test.expectedError, actual.Err))
				}
			})
		}
	}
}