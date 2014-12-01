// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import "testing"

func TestMarshal(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	c, err := db.Dial()
	if err != nil {
		t.Fatal(err)
	}

	c.Marshal([]byte("titi"), "toto")
}
