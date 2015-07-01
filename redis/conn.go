// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// DefaultMaximumConcurrentRequests defines the default maximum number of concurrent in-flight requests that can be sent to the Redis database.
var DefaultMaximumConcurrentRequests = 1000

// DefaultMaximumPendingRequests defines the default maximum number of requests that can be queued before blocking.
var DefaultMaximumPendingRequests = 1000

// DefaultMaximumConnectionRetries defines the number of times the client will try to connect to the Redis database before giving up.
var DefaultMaximumConnectionRetries = 3

// DefaultRetryTimeout defines the duration multiplicatively increased to provide exponential backoff delay when connecting to the Redis database.
var DefaultRetryTimeout = time.Second

// Conn implements a client connection to the Redis database.
type Conn struct {
	MaximumConcurrentRequests int
	MaximumPendingRequests    int
	MaximumConnectionRetries  int
	RetryTimeout              time.Duration

	db  dialer
	lua map[string]string

	feed chan *Request
	conn *net.Conn
	once sync.Once
	wg   sync.WaitGroup
}

type dialerFunc func() (net.Conn, error)

func (f dialerFunc) dial() (net.Conn, error) {
	return f()
}

type dialer interface {
	dial() (net.Conn, error)
}

func (conn *Conn) process() {
	pending := conn.MaximumPendingRequests
	if 0 == pending {
		pending = DefaultMaximumPendingRequests
	}

	conn.feed = make(chan *Request, pending)

	requests := conn.MaximumConcurrentRequests
	if 0 == requests {
		requests = DefaultMaximumConcurrentRequests
	}

	// start background workers to send and receive requests
	conn.wg.Add(1)
	go func() {
		read := make(chan func(), requests)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			for f := range read {
				f()
			}

			wg.Done()
		}()

		retries := conn.MaximumConnectionRetries
		if 0 == retries {
			retries = DefaultMaximumConnectionRetries
		}

		timeout := conn.RetryTimeout
		if 0 == timeout {
			timeout = DefaultRetryTimeout
		}

		var encoder *Encoder
		var decoder *Decoder

		// try to connect for the first time
		fd, err := conn.connect()

		// when in fail state, all pending commands are purged
		fail := false

		for cmd := range conn.feed {
			if fail {
				if fail = cmd != nil; fail {
					cmd.err = err
					close(cmd.done)
				}

				continue
			}

			c := cmd
			n := 0

			for n < retries {
				// encode and send the request over the network
				if fd != nil {
					if encoder == nil {
						encoder = NewEncoder(fd)
					}

					err = c.encode(encoder)
				}

				// handle errors by reconnecting
				if err != nil {
					if fd != nil {
						fd.Close()
					}

					n++
					time.Sleep(time.Duration(int64(n) * int64(timeout)))
					fd, err = conn.connect()
					c.err = err
					continue
				}

				if decoder == nil {
					decoder = NewDecoder(fd)
				}

				// enqueue the decoding of the response to the request
				d := decoder
				read <- func() {
					c.decode(d)
					close(c.done)
				}

				n = 0
				break
			}

			// enter fail mode to purge pending requests
			if n != 0 {
				close(c.done)

				fail = true
				go func() {
					conn.feed <- nil
				}()

				break
			}
		}

		close(read)
		wg.Wait()
		conn.wg.Done()
	}()

	return
}

// LuaScript loads a script into the script cache.
func (conn *Conn) LuaScript(code string) (id string, err error) {
	result, err := conn.Do("SCRIPT", "LOAD", code)
	if err != nil {
		return
	}

	if conn.lua == nil {
		conn.lua = make(map[string]string)
	}

	id = string(result.([]byte))
	conn.lua[id] = code
	return
}

// Close tears down the connection to the Redis database.
func (conn *Conn) Close() {
	if conn == nil {
		return
	}

	close(conn.feed)
	conn.wg.Wait()
}

// Do executes the specified command (with optional arguments) to the Redis instance and waits to decode the reply.
func (conn *Conn) Do(name string, args ...interface{}) (result interface{}, err error) {
	request := NewRequest(name, args...)
	if err = conn.Send(request); err == nil {
		result = request.commands[len(request.commands)-1].result
	}

	return
}

// Send sends the specified request to the Redis instance and waits for the reply.
func (conn *Conn) Send(request *Request) error {
	conn.once.Do(conn.process)
	request.done = make(chan struct{})
	conn.feed <- request
	<-request.done
	return request.err
}

func (conn *Conn) connect() (result net.Conn, err error) {
	c, err := conn.db.dial()
	if err != nil {
		return
	}

	// load lua scripts when needed
	n := len(conn.lua)
	if n != 0 {
		ids := make([]string, 0, n)

		// work directly on the stream to bypass everything
		encoder := NewEncoder(c)
		decoder := NewDecoder(c)

		// send all scripts commands at once
		for key, code := range conn.lua {
			encoder.Encode("SCRIPT", "LOAD", code)
			ids = append(ids, key)
		}

		// wait for the result of each command
		for i := 0; i < n; i++ {
			var reply interface{}
			reply, err = decoder.Decode()
			if err != nil {
				return
			}

			id := string(reply.([]byte))
			if id != ids[i] {
				err = fmt.Errorf("script SHA1 doesn't match '%s' vs. '%s'", ids[i], id)
				return
			}
		}
	}

	result = c
	return
}

// Dial connects to a Redis database instance at the specified address on the named network.
func Dial(network, address string) *Conn {
	return &Conn{
		db: dialerFunc(func() (net.Conn, error) {
			return net.Dial(network, address)
		}),
	}
}

// DialTimeout connects to a Redis database instance at the specified address on the named network with a timeout.
func DialTimeout(network, address string, timeout time.Duration) *Conn {
	return &Conn{
		db: dialerFunc(func() (net.Conn, error) {
			return net.DialTimeout(network, address, timeout)
		}),
	}
}
