// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// DB controls a Redis database instance.
type DB struct {
	cmd  *exec.Cmd
	path string
}

// NewTestDB creates a temporary Redis database instance in a temporary directory that can be used for testing.
// Communication is done via an unix socket to avoid conflicts with allocated ports.
func NewTestDB() (result *DB, err error) {
	path, err := ioutil.TempDir("", "redis")
	if err != nil {
		return
	}

	filename := path + "/redis.socket"
	cmd := exec.Command("redis-server", "--port", "0", "--unixsocket", filename, "--dir", path)

	db := &DB{
		cmd:  cmd,
		path: path,
	}

	defer func() {
		db.Close()
	}()

	err = cmd.Start()
	if err != nil {
		return
	}

	for i := 0; i != 100; i++ {
		if _, err = os.Stat(filename); err == nil {
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	result, db = db, nil
	return
}

// Network returns the network name that should be used to dial the server.
func (db *DB) Network() string {
	return "unix"
}

// String returns the address that should be used to dial the server.
func (db *DB) String() string {
	return db.path + "/redis.socket"
}

// Close terminates the Redis database instance and removes any temporary data that was created.
func (db *DB) Close() {
	if db == nil {
		return
	}

	if db.cmd != nil {
		if err := db.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			panic(err)
		}

		if err := db.cmd.Wait(); err != nil {
			panic(err)
		}
	}

	if db.path != "" {
		err := os.RemoveAll(db.path)
		if err != nil {
			panic(err)
		}
	}
}
