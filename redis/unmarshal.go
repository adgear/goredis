// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// OK represents the +OK string returned by many Redis commands.
var OK interface{} = "+OK"

type Decoder struct {
	reader *bufio.Reader
}

func NewDecoder(reader io.Reader) (result *Decoder) {
	result = &Decoder{
		reader: bufio.NewReader(reader),
	}

	return
}

func (decoder *Decoder) getLine() (result string, err error) {
	line, err := decoder.reader.ReadString('\n')
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

func (decoder *Decoder) get() (result interface{}, err error) {
	line, err := decoder.getLine()
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

		_, err = io.ReadFull(decoder.reader, reply)
		if err != nil {
			return
		}

		_, err = decoder.getLine()
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
			reply[i], err = decoder.get()
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

// Get decodes the reply of the Redis instance for a command that was sent.
func (decoder *Decoder) Decode() (result interface{}, err error) {
	result, err = decoder.get()
	return
}
