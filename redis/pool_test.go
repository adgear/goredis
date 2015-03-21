// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"fmt"
	"testing"
)

func TestPool(t *testing.T) {
	redis, err := New("", map[string]string{"port": "6366"})
	if err != nil {
		t.Fatal(err)
	}

	pool, err := NewPool(0, "tcp", "127.0.0.1:6366")
	if err != nil {
		fmt.Println(err)
	}

	pool.PrintState()

	reply, err := pool.Do("SET", "one", "1")
	if err != nil {
		t.Error(err)
	}

	reply, err = pool.Do("GET", "one")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v\n", reply)

	pool.PrintState()

	for i := 0; i < 100; i++ {
		go func() {
			reply, err = pool.Do("GET", "one")
			if err != nil {
				t.Error(err)
			}
			fmt.Printf("%v\n", reply)
			pool.PrintState()
		}()
	}

	pool.PrintState()
	redis.Close()
}
