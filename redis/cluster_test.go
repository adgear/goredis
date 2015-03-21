// Copyright (c) 2015 Datacratic. All rights reserved.

package redis

import "testing"

func TestHash(t *testing.T) {
	if crc16([]byte("123456789")) != 0x31C3 {
		t.Fail()
	}
}

func BenchmarkHash(b *testing.B) {
	text := []string{
		"123456789",
		"987654321",
		"abcdefghijklmnopqrstuvwxyz",
	}

	k := 0
	for i := 0; i != b.N; i++ {
		h := crc16([]byte(text[i%len(text)]))
		k += int(h)
	}
}

func TestSlot(t *testing.T) {
	hash := func(key []byte) int {
		return int(crc16(key)) % 16384
	}

	test := func(a, b string) {
		g := slot([]byte(a))
		h := hash([]byte(b))
		if g != h {
			t.Fatalf("unexpected result for '%s' slot=%d hash=%d of '%s'", a, g, h, b)
		}
	}

	test("{foo}bar", "foo")
	test("{foo}bla", "foo")
	test("foo{bar}", "bar")
	test("foo{}bar", "foo{}bar")
	test("foo{}{bar}", "foo{}{bar}")
	test("foo{{bar}}", "{bar")
	test("foo{bar}{bla}", "bar")
	test("{foobar}", "foobar")
	test("foobar{", "foobar{")
	test("{foobar", "{foobar")
}

func TestCluster(t *testing.T) {
	if !clusterSupported() {
		t.Skip("redis-server doesn't support clusters")
		return
	}

	n := 3
	cluster, err := NewTestCluster(n)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("cluster is up with %d nodes\n", n)
	defer cluster.Close()

	client := cluster.Dial()
	defer client.Close()

	test := func(a, b string) {
		if result, err := client.Do("SET", a, b); err != nil {
			t.Fatal(err, result)
		}

		if result, err := client.Do("GET", a); err != nil {
			t.Fatal(err, result)
		} else {
			text := string(result.([]byte))
			if text != b {
				t.Fatalf("unexpected result '%v' instead of '%s'", result, b)
			}
		}
	}

	test("hello", "world!")
	test("hello world!", "bof")
	test("{foo}bar", "foo")
	test("foo{bar}", "bar")
}
