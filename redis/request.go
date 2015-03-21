// Copyright (c) 2015 Datacratic. All rights reserved.

package redis

import "strings"

type command struct {
	name   string
	args   []interface{}
	err    error
	result interface{}
}

// Request defines a set of Redis commands that must be executed in sequence.
type Request struct {
	commands []command
	key      []byte
	hash     int
	err      error
	moved    bool
	redirect bool
	address  string
	done     chan struct{}
}

// NewRequest creates a new request that holds the specified command.
func NewRequest(name string, args ...interface{}) *Request {
	return &Request{
		commands: []command{
			command{
				name: name,
				args: args,
			},
		},
	}
}

// Len returns the number of commands in the request.
func (request *Request) Len() int {
	return len(request.commands)
}

// Add appends the specified command to the request.
func (request *Request) Add(name string, args ...interface{}) {
	request.commands = append(request.commands, command{
		name: name,
		args: args,
	})
}

func (request *Request) encode(encoder *Encoder) (err error) {
	for i := range request.commands {
		err = request.commands[i].encode(encoder)
		if err != nil {
			break
		}
	}

	request.err = err
	return
}

func (cmd *command) encode(encoder *Encoder) error {
	return encoder.Encode(cmd.name, cmd.args...)
}

func (request *Request) decode(decoder *Decoder) (err error) {
	for i := range request.commands {
		err = request.commands[i].decode(decoder)
		if err != nil {
			break
		}
	}

	if err != nil {
		result, ok := request.commands[0].result.(string)
		if ok {
			if strings.HasPrefix(result, "MOVED") {
				request.moved = true
			}

			request.redirect = request.moved || strings.HasPrefix(result, "ASK")
			if request.redirect {
				request.address = result[strings.LastIndex(result, " ")+1:]
			}
		}
	}

	request.err = err
	return
}

func (cmd *command) decode(decoder *Decoder) error {
	cmd.result, cmd.err = decoder.Decode()
	return cmd.err
}

// Sender is implemented to support sending requests.
type Sender interface {
	Send(*Request) error
}

// Send sends a request.
func (request *Request) Send(s Sender) error {
	return s.Send(request)
}

// Result returns the reply and/or the error received.
func (request *Request) Result(i int) (interface{}, error) {
	r := &request.commands[i]
	return r.result, r.err
}

func (request *Request) slot() int {
	if request.key == nil {
		request.key = []byte(request.commands[0].args[0].(string))
		request.hash = slot(request.key)
	}

	return request.hash
}
