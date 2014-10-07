// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"reflect"
	"testing"
)

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
			i int
			b bool
		}{
			i: 1,
			b: true,
		}},
		OK,
	},
	{
		[]interface{}{"GET", "obj"},
		[]byte("{1 true}"),
	},
}

func TestCommands(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	c, err := db.Dial()
	if err != nil {
		t.Fatal(err)
	}

	for i, cmd := range testCommands {
		t.Logf("%d: '%v'\n", i, cmd.args)

		result, err := c.Do(cmd.args[0].(string), cmd.args[1:]...)
		if err != nil {
			t.Errorf("unexpected result '%s'", err)
			continue
		}

		if !reflect.DeepEqual(result, cmd.expected) {
			t.Errorf("unexpected result '%v' instead of '%v'", result, cmd.expected)
		}
	}

	result, err := c.Do("LPUSH", "obj", "bad")
	if err == nil {
		t.Errorf("unexpected result '%v'", result)
	}
}
