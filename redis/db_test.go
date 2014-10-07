// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import "testing"

func BenchmarkDB(b *testing.B) {
	db, err := NewTestDB()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	c, err := db.Dial()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := c.Do("LPUSH", "queue", i)
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

	c, err := db.Dial()
	if err != nil {
		b.Fatal(err)
	}

	done := make(chan int)

	go func() {
		for i := 0; i < b.N; i++ {
			result, err := c.Get()
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
		if err := c.Put("LPUSH", "queue", i); err != nil {
			b.Fatal(err)
		}
	}

	if err = c.Flush(); err != nil {
		b.Fail()
	}

	if n := <-done; n != b.N {
		b.Fail()
	}
}
