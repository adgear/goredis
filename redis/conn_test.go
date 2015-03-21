// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

type noDB struct{}

func (db *noDB) dial() (conn net.Conn, err error) {
	err = fmt.Errorf("no db")
	return
}

func TestNoConnection(t *testing.T) {
	conn := &Conn{
		db:           new(noDB),
		RetryTimeout: time.Millisecond,
	}

	if result, err := conn.Do("PING"); err == nil || result != nil {
		t.Fatal(err, result)
	}
}

func TestConnectionDropped(t *testing.T) {
	db := new(mockDB)
	conn := &Conn{db: db}

	db.result.WriteString("+PONG\r\n")
	if result, err := conn.Do("PING"); err != nil || result != "PONG" {
		t.Fatal(err, result)
	}

	db.result.WriteString("+PONG\r\n")
	if result, err := conn.Do("PING"); err != nil || result != "PONG" {
		t.Fatal(err, result)
	}

	db.err = fmt.Errorf("failure")
	if result, err := conn.Do("PING"); err == nil || result != nil {
		t.Fatal(err, result)
	}

	db.result.WriteString("+PONG\r\n")
	db.err = nil
	if result, err := conn.Do("PING"); err != nil || result != "PONG" {
		t.Fatal(err, result)
	}
}

var testCommands = []struct {
	args     []interface{}
	expected interface{}
}{
	{
		[]interface{}{"PING"},
		"PONG",
	},
	{
		[]interface{}{"SET", "foo", "bar"},
		OK,
	},
	{
		[]interface{}{"GET", "foo"},
		[]byte("bar"),
	},
	{
		[]interface{}{"GET", "bar"},
		nil,
	},
	{
		[]interface{}{"MGET", "bar", "foo"},
		[]interface{}{nil, []byte("bar")},
	},
	{
		[]interface{}{"LPUSH", "list", "hello"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "list", "world"},
		int64(2),
	},
	{
		[]interface{}{"LPUSH", "list", "baby"},
		int64(3),
	},
	{
		[]interface{}{"LPOP", "list"},
		[]byte("baby"),
	},
	{
		[]interface{}{"LRANGE", "list", 0, -1},
		[]interface{}{[]byte("world"), []byte("hello")},
	},
	{
		[]interface{}{"MULTI"},
		OK,
	},
	{
		[]interface{}{"INCR", "count"},
		"QUEUED",
	},
	{
		[]interface{}{"MSET", "a", 42, "b", 3.1415},
		"QUEUED",
	},
	{
		[]interface{}{"MGET", "a", "b"},
		"QUEUED",
	},
	{
		[]interface{}{"EXEC"},
		[]interface{}{
			int64(1),
			OK,
			[]interface{}{[]byte("42"), []byte("3.1415")},
		},
	},
	{
		[]interface{}{"SET", "obj", struct {
			N int
			B bool
		}{
			N: 1,
			B: true,
		}},
		OK,
	},
	{
		[]interface{}{"GET", "obj"},
		[]byte(`{"N":1,"B":true}`),
	},
}

func TestCommands(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	conn := db.Dial()
	defer conn.Close()

	for i, cmd := range testCommands {
		t.Logf("%d: '%v'\n", i, cmd.args)

		result, err := conn.Do(cmd.args[0].(string), cmd.args[1:]...)
		if err != nil {
			t.Errorf("unexpected result '%s'", err)
			continue
		}

		if !reflect.DeepEqual(result, cmd.expected) {
			t.Errorf("unexpected result '%v' instead of '%v'", result, cmd.expected)
		}
	}

	result, err := conn.Do("LPUSH", "obj", "bad")
	if err == nil {
		t.Errorf("unexpected result '%v'", result)
	}
}

func BenchmarkConn(b *testing.B) {
	db, err := NewTestDB()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	conn := &Conn{
		db: db,
	}

	defer conn.Close()
	var wg sync.WaitGroup

	// create enough workers to process everything
	n := 1000
	send := make(chan func(), b.N)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for f := range send {
				f()
			}

			wg.Done()
		}()
	}

	b.ResetTimer()

	// use the workers to perform all requests
	for i := 0; i < b.N; i++ {
		k := i
		send <- func() {
			_, err := conn.Do("LPUSH", "queue", k)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	close(send)
	wg.Wait()
}
