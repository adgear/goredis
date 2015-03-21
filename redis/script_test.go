// Copyright (c) 2015 Datacratic. All rights reserved.

package redis

import (
	"testing"
)

func TestScript(t *testing.T) {
	db, err := NewTestDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	conn := db.Dial()
	defer conn.Close()

	script := `
		local value = redis.call("GET", KEYS[1])
		if not value then
			value = "bar"
			redis.call("SET", KEYS[1], ARGV[1] .. value)
		end
		return value
	`

	id, err := conn.LuaScript(script)
	if err != nil {
		t.Fatal(err)
	}

	if result, err := conn.Do("EVALSHA", id, 1, "test", "foo"); err != nil {
		t.Fatal(err)
	} else {
		if string(result.([]byte)) != "bar" {
			t.Fatal(result)
		}
	}

	if result, err := conn.Do("EVALSHA", id, 1, "test", "foo"); err != nil {
		t.Fatal(err)
	} else {
		if string(result.([]byte)) != "foobar" {
			t.Fatal(result)
		}
	}
}
