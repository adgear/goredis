package redis

import (
	"fmt"
	"sync"
)

type Pool struct {
	network     string
	address     string
	connections []*Conn
	free        []*Conn
	sync.Mutex
}

func NewPool(numConn int, network, address string) (*Pool, error) {
	pool := Pool{network: network, address: address, connections: []*Conn{}, free: []*Conn{}}
	for i := 0; i < numConn; i++ {
		pool.Lock()
		if err := pool.add(); err != nil {
			return nil, err
		}
		pool.Unlock()
	}
	return &pool, nil
}

func (pool *Pool) Do(command string, args ...interface{}) (interface{}, error) {

	if conn, err := pool.get(); err != nil {
		return nil, err
	} else {
		reply, err := conn.Do(command, args...)
		pool.release(conn)
		return reply, err
	}
}

func (pool *Pool) Close() {
	for _, conn := range pool.connections {
		conn.Close()
	}
}

func (pool *Pool) PrintState() {
	pool.Lock()
	numConn := len(pool.connections)
	numFree := len(pool.free)
	pool.Unlock()
	fmt.Println("conn:", numConn, "free:", numFree)
}

func (pool *Pool) get() (*Conn, error) {
	pool.Lock()
	defer pool.Unlock()
	if len(pool.free) == 0 {
		if err := pool.add(); err != nil {
			return nil, err
		}
	}
	conn := pool.free[0]
	pool.free = pool.free[1:]
	return conn, nil
}

func (pool *Pool) release(conn *Conn) {
	pool.Lock()
	defer pool.Unlock()
	if len(pool.free) > (len(pool.connections)/2)+1 {
		for i, c := range pool.connections {
			if c == conn {
				pool.connections = append(pool.connections[:i], pool.connections[i+1:]...)
				break
			}
		}
		conn.Close()
	} else {
		pool.free = append(pool.free, conn)
	}
}

func (pool *Pool) add() error {
	conn := Dial(pool.network, pool.address)
	pool.connections = append(pool.connections, conn)
	pool.free = append(pool.free, conn)
	return nil
}
