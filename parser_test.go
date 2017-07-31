package main

import (
	"testing"
	"fmt"
	"strings"
)

func Test_Parser_Should_Parse_Points(t *testing.T) {
	tests := []struct {
		label			string
		input 			string
	}{
		{
			"When Parsing 1 Valid Point",
			"ceph_pgmap_state,host=ceph-mon-0,state=active+clean count=22952 1468928660000000000",
		},
		{
			"When Parsing 1 Valid Point With No Timestamp",
			"ceph_pgmap_state,host=ceph-mon-0,state=active+clean count=22952",
		},
	}

	sut := NewParser()
	for _, test := range tests {
		input := []byte(test.input)
		parsed := sut.parse(input)
		if parsed.Err != nil {
			t.Fatal("%s: failed to parsing %s with error: %s", test.label, test.input, parsed.Err.Error())
		}
		if !strings.Contains(parsed.Point.String(), test.input) {
			t.Error(fmt.Sprintf("Error: failed to parse point, expected: %s, actual: %s", test.input, parsed.Point.String()))
		}
	}
}