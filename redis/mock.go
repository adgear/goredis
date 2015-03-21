// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"bytes"
	"net"
	"time"
)

type mockDB struct {
	err    error
	result bytes.Buffer
}

func (db *mockDB) dial() (conn net.Conn, err error) {
	conn = &mockConn{db: db}
	return
}

type mockConn struct {
	db *mockDB
}

func (conn *mockConn) Read(b []byte) (n int, err error) {
	if err = conn.db.err; err != nil {
		return
	}

	n, err = conn.db.result.Read(b)
	return
}

func (conn *mockConn) Write(b []byte) (n int, err error) {
	n = len(b)
	return
}

func (conn *mockConn) Close() (err error) {
	return
}

func (conn *mockConn) LocalAddr() (addr net.Addr) {
	return
}

func (conn *mockConn) RemoteAddr() (addr net.Addr) {
	return
}

func (conn *mockConn) SetDeadline(t time.Time) (err error) {
	return
}

func (conn *mockConn) SetReadDeadline(t time.Time) (err error) {
	return
}

func (conn *mockConn) SetWriteDeadline(t time.Time) (err error) {
	return
}
