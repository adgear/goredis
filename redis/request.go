// Copyright (c) 2015 Datacratic. All rights reserved.

package redis

type command struct {
	name   string
	args   []interface{}
	err    error
	result interface{}
}

type Request struct {
	commands []command
	err      error
	done     chan struct{}
}

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

func (request *Request) Len() int {
	return len(request.commands)
}

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

	request.err = err
	return
}

func (cmd *command) decode(decoder *Decoder) error {
	cmd.result, cmd.err = decoder.Decode()
	return cmd.err
}

type Sender interface {
	Send(*Request) error
}

func (request *Request) Send(s Sender) error {
	return s.Send(request)
}

func (request *Request) Result(i int) (interface{}, error) {
	r := &request.commands[i]
	return r.result, r.err
}

/*
func (request *Request) slot() (result int) {
	return
}

func (request *Request) redirect() (result bool) {
	return
}

func (request *Request) moved() (result bool) {
	return
}

func (request *Request) address() (result string) {
	return
}

func (request *Request) fail(err error) {
	request.commands[0].err = err
}
*/
