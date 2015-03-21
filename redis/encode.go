// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strconv"
)

// Encoder implements the encoding part of the Redis serialization protocol.
type Encoder struct {
	// writer adds some buffering to the output.
	writer *bufio.Writer

	// scratch is a preallocated buffer used to format both numbers and len.
	scratch [64]byte
}

// NewEncoder creates a RESP encoder to the specified writer source.
func NewEncoder(writer io.Writer) (result *Encoder) {
	result = &Encoder{
		writer: bufio.NewWriter(writer),
	}

	// assume the end of the scratch buffer will always be correctly terminated
	result.scratch[len(result.scratch)-2] = '\r'
	result.scratch[len(result.scratch)-1] = '\n'
	return
}

func (encoder *Encoder) putLen(prefix byte, k int) (err error) {
	i := len(encoder.scratch) - 3
	for {
		if k < 10 {
			encoder.scratch[i] = byte('0' + k)
			i--
			break
		}

		encoder.scratch[i] = byte('0' + k%10)
		i--

		k = k / 10
	}

	encoder.scratch[i] = prefix
	_, err = encoder.writer.Write(encoder.scratch[i:])
	return
}

func (encoder *Encoder) putBytes(data []byte) (err error) {
	encoder.putLen('$', len(data))
	encoder.writer.Write(data)
	_, err = encoder.writer.WriteString("\r\n")
	return
}

func (encoder *Encoder) putString(value string) (err error) {
	encoder.putLen('$', len(value))
	encoder.writer.WriteString(value)
	_, err = encoder.writer.WriteString("\r\n")
	return
}

func (encoder *Encoder) putInt(k int64) error {
	result := strconv.AppendInt(encoder.scratch[:0], k, 10)
	return encoder.putBytes(result)
}

func (encoder *Encoder) putFloat(k float64) error {
	result := strconv.AppendFloat(encoder.scratch[:0], k, 'g', -1, 64)
	return encoder.putBytes(result)
}

func (encoder *Encoder) put(cmd string, args []interface{}) (err error) {
	encoder.putLen('*', len(args)+1)
	err = encoder.putString(cmd)

	for _, arg := range args {
		if err != nil {
			break
		}

		switch arg := arg.(type) {
		case []byte:
			err = encoder.putBytes(arg)
		case string:
			err = encoder.putString(arg)
		case int:
			err = encoder.putInt(int64(arg))
		case int32:
			err = encoder.putInt(int64(arg))
		case int64:
			err = encoder.putInt(arg)
		case float32:
			err = encoder.putFloat(float64(arg))
		case float64:
			err = encoder.putFloat(arg)
		case bool:
			if arg {
				err = encoder.putString("1")
			} else {
				err = encoder.putString("0")
			}
		case nil:
			err = encoder.putString("")
		default:
			var data []byte
			if marshaler, ok := arg.(Marshaler); ok {
				data, err = marshaler.MarshalREDIS()
			} else {
				data, err = json.Marshal(arg)
			}

			if err == nil {
				err = encoder.putBytes(data)
			}
		}
	}

	return
}

// Encode writes the specified command and arguments.
func (encoder *Encoder) Encode(command string, args ...interface{}) (err error) {
	err = encoder.put(command, args)
	if err != nil {
		return
	}

	err = encoder.writer.Flush()
	return
}

// Marshaler is implemented by objects that want to marshal their Redis representation.
type Marshaler interface {
	MarshalREDIS() ([]byte, error)
}

// Marshal encodes the command and arguments.
func Marshal(command string, args ...interface{}) (result []byte, err error) {
	buffer := &bytes.Buffer{}
	err = NewEncoder(buffer).Encode(command, args...)
	if err != nil {
		return
	}

	result = buffer.Bytes()
	return
}
