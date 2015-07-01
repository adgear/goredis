// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// DB defines a local Redis database instance.
type DB struct {
	cmd *exec.Cmd
	dir string
	ipc string
	end chan struct{}
}

// New creates a local Redis database instance.
// Communication is done via a unix socket.
// Set "port" to "0" in the config to avoid conflicts with allocated ports when needed.
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
		end: make(chan struct{}),
	}

	defer func() {
		db.Close()
	}()

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return
	}

	end := db.end
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf(scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			log.Printf("error while running '%s': %s\n", path, err)
		}

		close(end)
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

	conn := db.Dial()

	reply, err := conn.Do("QUIT")
	if err != nil {
		return
	}

	if reply != OK {
		err = fmt.Errorf("failed to start Redis instance")
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

func (db *DB) dial() (net.Conn, error) {
	return net.Dial("unix", db.ipc)
}

// Dial connects directly to the Redis database instance.
func (db *DB) Dial() *Conn {
	return &Conn{
		db: db,
	}
}

// URL returns a network/address pair encoded as scheme://host where scheme is the network and host is the address.
func (db *DB) URL() string {
	return "unix://" + db.ipc
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

		<-db.end

		if err := db.cmd.Wait(); err != nil {
			exit, ok := err.(*exec.ExitError)
			if !ok {
				panic(err)
			}

			log.Printf("redis-server: %s\n", exit)
		}
	}

	if db.dir != "" {
		err := os.RemoveAll(db.dir)
		if err != nil {
			panic(err)
		}
	}
}
