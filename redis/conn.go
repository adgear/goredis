// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"net"
	"time"
)

// Conn implements a client connection to the Redis database.
type Conn struct {
	conn    net.Conn
	encoder *Encoder
	decoder *Decoder
}

// Dial connects to a Redis database instance at the specified address on the named network.
// See net.Dial for more details.
func Dial(network, address string) (result *Conn, err error) {
	conn, err := net.Dial(network, address)
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

// DialTimeout connects to a Redis database instance at the specified address on the named network with a timeout.
// See net.DialTimeout for more details.
func DialTimeout(network, address string, timeout time.Duration) (result *Conn, err error) {
	conn, err := net.DialTimeout(network, address, timeout)
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

// Close closes the connection.
func (conn *Conn) Close() (err error) {
	if conn == nil || conn.conn == nil {
		return
	}

	conn.conn.Close()
	return
}

// Do sends the specified command (and arguments) to the Redis instance and decodes the reply.
// The Redis command reference (http://redis.io/commands) lists the available commands.
func (conn *Conn) Do(command string, args ...interface{}) (result interface{}, err error) {
	if err = conn.Put(command, args...); err != nil {
		return
	}

	result, err = conn.Get()
	return
}

// Put send the specified command (and arguments) to the Redis instance.
// The Redis command reference (http://redis.io/commands) lists the available commands.
func (conn *Conn) Put(command string, args ...interface{}) (err error) {
	err = conn.encoder.Encode(command, args...)
	return
}

// Get decodes the reply of the Redis instance for a command that was sent.
func (conn *Conn) Get() (result interface{}, err error) {
	result, err = conn.decoder.Decode()
	return
}
