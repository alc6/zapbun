package db

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func postgreDSN(t *testing.T) string {
	t.Helper()

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_DATABASE"),
	)

	return dsn
}

func TestNewQueryHook(t *testing.T) {
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(postgreDSN(t))))
	db := bun.NewDB(sqldb, pgdialect.New())

	ts := newTestLogSpy(t)
	defer ts.AssertPassed()

	logger := zaptest.NewLogger(ts)

	defer func(t *testing.T) {
		require.NoError(t, db.Close())
	}(t)

	cases := []struct {
		description      string
		query            string
		expectedErrMsg   string
		messagesExpected []string
		setupDB          func()
	}{
		{
			description:      "Debug message logged",
			query:            "SELECT 1 AS ONE",
			expectedErrMsg:   "",
			messagesExpected: []string{"DEBUG\tSELECT 1 AS ONE"},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithVerbose(true))
				db.AddQueryHook(hook)
			},
		},
		{
			description:      "Error occurs",
			query:            "SELECT * FROM nop",
			expectedErrMsg:   "ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)",
			messagesExpected: []string{"ERROR\tSELECT * FROM nop error: ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)"},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithVerbose(true))
				db.AddQueryHook(hook)
			},
		},
		{
			description:      "Verbose disabled, no message logged",
			query:            "SELECT 1 AS ONE",
			expectedErrMsg:   "",
			messagesExpected: []string{},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithVerbose(false))
				db.AddQueryHook(hook)
			},
		},
		{
			description:      "Verbose disabled, error logged",
			query:            "SELECT * FROM nop",
			expectedErrMsg:   "ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)",
			messagesExpected: []string{"ERROR\tSELECT * FROM nop error: ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)"},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithVerbose(true))
				db.AddQueryHook(hook)
			},
		},
		{
			description:      "Hook disabled",
			query:            "SELECT * FROM nop",
			expectedErrMsg:   "ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)",
			messagesExpected: []string{},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithEnabled(false))
				db.AddQueryHook(hook)
			},
		},
		{
			description:      "Error as field",
			query:            "SELECT * FROM nop",
			expectedErrMsg:   "ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)",
			messagesExpected: []string{"ERROR\tSELECT * FROM nop\t{\"err\": \"ERROR: relation \\\"nop\\\" does not exist (SQLSTATE=42P01)\"}"},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithVerbose(true), WithErrorAsField("err"))
				db.AddQueryHook(hook)
			},
		},
		{
			description:      "Custom level: err as warning",
			query:            "SELECT * FROM nop",
			expectedErrMsg:   "ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)",
			messagesExpected: []string{"WARN\tSELECT * FROM nop error: ERROR: relation \"nop\" does not exist (SQLSTATE=42P01)"},
			setupDB: func() {
				db = bun.NewDB(sqldb, pgdialect.New())
				hook := NewQueryHook(logger, WithVerbose(true), WithLevels(zap.InfoLevel, zap.WarnLevel))
				db.AddQueryHook(hook)
			},
		},
	}

	for _, tc := range cases {
		tc.setupDB()

		_, err := db.Query(tc.query)

		if tc.expectedErrMsg != "" {
			assert.Equal(t, tc.expectedErrMsg, err.Error(), tc.description)
		}
		ts.AssertMessages(tc.description, tc.messagesExpected...)

		ts.flushMessages()
	}
}

// TestNewQueryHook_Duration gives a special treatment to duration cases as they vary in the CI.
// Test does not go deep.
func TestNewQueryHook_Duration(t *testing.T) {
	const description = "Testing duration"

	hook := NewQueryHook(nil, WithDuration(), WithDurationAsField())

	assert.True(t, hook.duration, description)
	assert.True(t, hook.durationAsField, description)
}

// Below code from github.com/uber-go/zap/zaptest as a very handy helper func for tests.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// testLogSpy is a testing.TB that captures logged messages.
type testLogSpy struct {
	testing.TB

	failed   bool
	Messages []string
}

func newTestLogSpy(t testing.TB) *testLogSpy {
	return &testLogSpy{TB: t}
}

func (t *testLogSpy) Fail() {
	t.failed = true
}

func (t *testLogSpy) Failed() bool {
	return t.failed
}

func (t *testLogSpy) FailNow() {
	t.Fail()
	t.TB.FailNow()
}

func (t *testLogSpy) Logf(format string, args ...interface{}) {
	// Log messages are in the format,
	//
	//   2017-10-27T13:03:01.000-0700	DEBUG	your message here	{data here}
	//
	// We strip the first part of these messages because we can't really test
	// for the timestamp from these tests.
	m := fmt.Sprintf(format, args...)
	m = m[strings.IndexByte(m, '\t')+1:]
	t.Messages = append(t.Messages, m)
	t.TB.Log(m)
}

func (t *testLogSpy) AssertMessages(description string, msgs ...string) {
	assert.Equal(t.TB, msgs, t.Messages, description)
}

func (t *testLogSpy) AssertPassed() {
	t.assertFailed(false, "expected test to pass")
}

func (t *testLogSpy) AssertFailed() {
	t.assertFailed(true, "expected test to fail")
}

func (t *testLogSpy) assertFailed(v bool, msg string) {
	assert.Equal(t.TB, v, t.failed, msg)
}

func (t *testLogSpy) flushMessages() {
	t.Messages = []string{}
}
