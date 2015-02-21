// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// DB controls a Redis database instance.
type DB struct {
	cmd *exec.Cmd
	dir string
	ipc string
}

// New creates a local Redis database instance.
// Communication is done via a unix socket.
// Set "port" to "0" in the config to avoid conflicts with allocated ports.
func New(path string, config map[string]string) (result *DB, err error) {
	dir, err := ioutil.TempDir("", "redis")
	if err != nil {
		return
	}

	ipc := fmt.Sprintf("%s/redis-%d.socket", dir, rand.Uint32())
	config["unixsocket"] = ipc

	if path == "" {
		path = os.Getenv("REDIS")
		if path == "" {
			path = "redis-server"
		}
	}

	cmd := exec.Command(path, "-")

	db := &DB{
		cmd: cmd,
		dir: dir,
		ipc: ipc,
	}

	defer func() {
		db.Close()
	}()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}

	err = cmd.Start()
	if err != nil {
		return
	}

	for key, value := range config {
		_, err = fmt.Fprintf(stdin, "%s %s\n", key, value)
		if err != nil {
			return
		}
	}

	stdin.Close()

	for i := 0; i != 100; i++ {
		if _, err = os.Stat(ipc); err == nil {
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	conn, err := db.Dial()
	if err != nil {
		return
	}

	reply, err := conn.Do("QUIT")
	if err != nil {
		return
	}

	if reply != OK {
		return
	}

	result, db = db, nil
	return
}

// NewTestDB creates a temporary Redis database instance in a temporary directory that can be used for testing.
func NewTestDB() (result *DB, err error) {
	dir, err := ioutil.TempDir("", "redis")
	if err != nil {
		return
	}

	config := map[string]string{
		"port": "0",
		"dir":  dir,
	}

	result, err = New("", config)
	return
}

// Dial connects directly to the Redis database instance.
func (db *DB) Dial() (result *Conn, err error) {
	conn, err := net.Dial("unix", db.ipc)
	if err != nil {
		return
	}

	result = &Conn{
		conn:    conn,
		encoder: NewEncoder(conn),
		decoder: NewDecoder(conn),
	}

	return
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

	if db.dir != "" {
		err := os.RemoveAll(db.dir)
		if err != nil {
			panic(err)
		}
	}
}
