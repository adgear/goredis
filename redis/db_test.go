// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import "testing"

func BenchmarkDB(b *testing.B) {
	db, err := NewTestDB()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	conn := db.Dial()
	defer conn.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := conn.Do("LPUSH", "queue", i)
		if err != nil {
			b.Fatal(err)
		}

		if int64(i)+1 != result.(int64) {
			b.Fatal(result)
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	db, err := NewTestDB()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	conn, err := db.dial()
	if err != nil {
		b.Fatal(err)
	}

	defer conn.Close()

	done := make(chan int)

	// This benchmark tries to find the maximum speed that we can get with pipelining
	// over the unix socket. It doesn't have bounds on in-flight and thus, simply
	// sends everything at once while receiving. The result here can be used to give
	// an idea of the speed that the Conn and the Client can reach when the number
	// of concurrent requests is large enough for the test.

	encoder := NewEncoder(conn)
	decoder := NewDecoder(conn)

	go func() {
		for i := 0; i < b.N; i++ {
			result, err := decoder.Decode()
			if err != nil {
				b.Fatal(err)
			}

			if int64(i)+1 != result.(int64) {
				b.Fatal(result)
			}
		}

		done <- b.N
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := encoder.Encode("LPUSH", "queue", i); err != nil {
			b.Fatal(err)
		}
	}

	if n := <-done; n != b.N {
		b.Fail()
	}
}
