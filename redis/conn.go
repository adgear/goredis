// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// OK represents the +OK string returned by many Redis commands.
var OK interface{} = "+OK"

// Conn implements a client connection to the Redis database.
type Conn struct {
	conn net.Conn

	// Wait indicates that LOADING errors from Redis will try again.
	Wait bool

	reader *bufio.Reader
	writer *bufio.Writer

	// scratch is a preallocated buffer used to format both numbers and len.
	scratch [64]byte
}

// Dial connects to a Redis database instance at the specified address on the named network.
// See net.Dial for more details.
func Dial(network, address string) (result Conn, err error) {
	conn, err := net.Dial(network, address)
	if err == nil {
		result = newConn(conn)
	}

	return
}

// DialTimeout connects to a Redis database instance at the specified address on the named network with a timeout.
// See net.DialTimeout for more details.
func DialTimeout(network, address string, timeout time.Duration) (result Conn, err error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		result = newConn(conn)
	}

	return
}

func newConn(conn net.Conn) Conn {
	result := Conn{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}

	// assume the end of the scratch buffer will always be correctly terminated
	result.scratch[len(result.scratch)-2] = '\r'
	result.scratch[len(result.scratch)-1] = '\n'
	return result
}

// Close closes the connection.
func (conn *Conn) Close() (err error) {
	if conn == nil || conn.conn == nil {
		return
	}

	conn.conn.Close()
	return
}

func (conn *Conn) putLen(prefix byte, k int) (err error) {
	i := len(conn.scratch) - 3

	for {
		if k < 10 {
			conn.scratch[i] = byte('0' + k)
			i--
			conn.scratch[i] = prefix
			_, err = conn.writer.Write(conn.scratch[i:])
			break
		}

		conn.scratch[i] = byte('0' + k%10)
		i--
		k = k / 10
	}

	return
}

func (conn *Conn) putBytes(data []byte) (err error) {
	conn.putLen('$', len(data))
	conn.writer.Write(data)
	_, err = conn.writer.WriteString("\r\n")
	return
}

func (conn *Conn) putString(value string) (err error) {
	conn.putLen('$', len(value))
	conn.writer.WriteString(value)
	_, err = conn.writer.WriteString("\r\n")
	return
}

func (conn *Conn) putInt(k int64) error {
	result := strconv.AppendInt(conn.scratch[:0], k, 10)
	return conn.putBytes(result)
}

func (conn *Conn) putFloat(k float64) error {
	result := strconv.AppendFloat(conn.scratch[:0], k, 'g', -1, 64)
	return conn.putBytes(result)
}

func (conn *Conn) putCommand(cmd string, args []interface{}) (err error) {
	conn.putLen('*', len(args)+1)
	err = conn.putString(cmd)

	for _, arg := range args {
		if err != nil {
			break
		}

		switch arg := arg.(type) {
		case []byte:
			err = conn.putBytes(arg)
		case string:
			err = conn.putString(arg)
		case int:
			err = conn.putInt(int64(arg))
		case int32:
			err = conn.putInt(int64(arg))
		case int64:
			err = conn.putInt(arg)
		case float32:
			err = conn.putFloat(float64(arg))
		case float64:
			err = conn.putFloat(arg)
		case bool:
			if arg {
				err = conn.putString("1")
			} else {
				err = conn.putString("0")
			}
		case nil:
			err = conn.putString("")
		default:
			data := bytes.Buffer{}
			fmt.Fprint(&data, arg)
			err = conn.putBytes(data.Bytes())
		}
	}

	return
}

func (conn *Conn) getLine() (result string, err error) {
	line, err := conn.reader.ReadString('\n')
	if err != nil {
		return
	}

	n := len(line)

	if n == 0 {
		err = errors.New("redis returned an empty line")
		return
	}

	if n < 2 {
		err = fmt.Errorf("redis return data with invalid terminator '%s'", []byte(line))
		return
	}

	if line[n-2] != '\r' && line[n-1] != '\n' {
		err = fmt.Errorf("redis return data with invalid terminator '%s'", []byte(line))
	}

	result = line[:n-2]
	return
}

func (conn *Conn) getReply() (result interface{}, err error) {
	line, err := conn.getLine()
	if err != nil {
		return
	}

	if len(line) == 0 {
		err = errors.New("redis returned nothing")
		return
	}

	switch line[0] {
	case '+':
		if len(line) == 3 && line[1] == 'O' && line[2] == 'K' {
			result = OK
			return
		}

		result = line[1:]
	case '-':
		result, err = line[1:], fmt.Errorf("redis returned an error: %s", line[1:])
	case ':':
		result, err = strconv.ParseInt(line[1:], 10, 64)
	case '$':
		var n int64
		n, err = strconv.ParseInt(line[1:], 10, 64)
		if n < 0 || err != nil {
			return
		}

		reply := make([]byte, n)

		_, err = io.ReadFull(conn.reader, reply)
		if err != nil {
			return
		}

		_, err = conn.getLine()
		if err != nil {
			return
		}

		result = reply
	case '*':
		var n int64
		n, err = strconv.ParseInt(line[1:], 10, 64)
		if n < 0 || err != nil {
			return
		}

		reply := make([]interface{}, n)
		for i := range reply {
			reply[i], err = conn.getReply()
			if err != nil {
				return
			}
		}

		result = reply
	default:
		err = fmt.Errorf("redis returned '%s'", line)
	}

	return
}

// Do sends the specified command (and arguments) to the Redis database and returns the received result.
// The Redis command reference (http://redis.io/commands) lists the available commands.
func (conn *Conn) Do(command string, args ...interface{}) (result interface{}, err error) {
	for {
		if err = conn.putCommand(command, args); err != nil {
			break
		}

		if err = conn.writer.Flush(); err != nil {
			break
		}

		result, err = conn.getReply()
		if err == nil {
			break
		}

		text, ok := result.(string)
		if !ok || !conn.Wait || !strings.HasPrefix(text, "LOADING") {
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	return
}
