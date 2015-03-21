// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"sync"
	"testing"
)

func BenchmarkClient(b *testing.B) {
	db, err := NewTestDB()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	client := db.Dial()
	defer client.Close()

	// create enough workers to process everything
	var wg sync.WaitGroup

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
			_, err := client.Do("LPUSH", "queue", k)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	close(send)
	wg.Wait()
}
