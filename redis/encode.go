// Copyright (c) 2014 Datacratic. All rights reserved.

package redis

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
)

var (
	encodeHSET  = []byte("*4\r\n$4\r\nHSET\r\n")
	encodeRPUSH = []byte("*3\r\n$5\r\nRPUSH\r\n")
	encodeSADD  = []byte("*3\r\n$4\r\nSADD\r\n")
	encodeSET   = []byte("*3\r\n$3\r\nSET\r\n")
)

func basicType(item reflect.Type) bool {
	return true
}

// Marshal encodes a value at the specified key in a Redis DB.
//
// Types that can be encoded directly into a single key/value pair are bool, int, float, string and []byte.
// They will be SET directly or HSET if they are fields of a structure.
// When inside arrays or slices, they are added to the tail of a list using RPUSH.
// More complex objects are also supported.
// For instance, the map type is stored by first adding keys with SADD.
// It then encodes the value at the key composed of both the key of the set (used for SADD) and the key of the map.
// Arrays and slices use a similar scheme.
// But instead of storing keys, it either store the values directly or map to keys built from indices.
//
// Note that commands are issued inside a MULTI transaction.
func (conn *Conn) Marshal(key []byte, value interface{}) (k int, err error) {
	k, err = conn.encode(encodeSET, key, nil, reflect.ValueOf(value))
	log.Printf("%d\n", k)
	return
}

func (conn *Conn) encode(cmd, key, suffix []byte, value reflect.Value) (k int, err error) {
	switch value.Kind() {
	case reflect.Struct:
		k, err = conn.encodeFields(key, suffix, value)
	case reflect.Slice, reflect.Array:
		k, err = conn.encodeItems(cmd, key, suffix, value)
	case reflect.Map:
		k, err = conn.encodeMapItems(key, suffix, value)
	case reflect.Interface, reflect.Ptr:
		zero := reflect.Zero(value.Type())
		if value != zero {
			k, err = conn.encode(cmd, key, suffix, value.Elem())
		} else {
			k, err = conn.encode(cmd, key, suffix, zero)
		}
	default:
		err = fmt.Errorf("redis: unsupported type: %s", value.Type())
	}

	return
}

func (conn *Conn) encodeFields(key, suffix []byte, value reflect.Value) (k int, err error) {
	t := value.Type()
	for i, n := 0, t.NumField(); i < n && err == nil; i++ {
		f := t.Field(i)
		r := f.Tag.Get("redis")
		v := value.Field(i)

		if r != "-" && (v != reflect.Zero(v.Type()) || !strings.Contains(r, "omitempty")) {
			name := f.Name

			if name == "" {
				name = f.Type.String()
			}

			if suffix == nil {
				k, err = conn.encode(encodeHSET, key, []byte(name), v)
			} else {
				k, err = conn.encode(encodeHSET, key, append(append(suffix, byte(':')), []byte(name)...), v)
			}
		}
	}

	return
}

func (conn *Conn) encodeItems(cmd, key, suffix []byte, items reflect.Value) (k int, err error) {
	if basicType(items.Type().Elem()) {
		for i, n := 0, items.Len(); i < n && err == nil; i++ {
			x := 0

			if x, err = conn.encode(encodeRPUSH, key, suffix, items.Index(i)); err != nil {
				return
			}

			k = k + x
		}
	} else {
		n := items.Len()
		k, err = conn.encode(cmd, key, suffix, reflect.ValueOf(n))
		for i := 0; i < n && err == nil; i++ {
			x := 0

			result := strconv.AppendInt(conn.scratch[:0], int64(i), 10)
			if x, err = conn.encode(encodeSET, append(append(key, byte(':')), result...), nil, items.Index(i)); err != nil {
				return
			}

			k = k + x
		}
	}

	return
}

func (conn *Conn) encodeMapItems(key, suffix []byte, items reflect.Value) (k int, err error) {
	set := key

	if suffix != nil {
		set = append(append(set, byte(':')), suffix...)
	}

	keys := items.MapKeys()
	for i, n := 0, len(keys); i < n && err == nil; i++ {
		x, y := 0, 0

		if x, err = conn.encode(encodeSADD, set, nil, keys[i]); err != nil {
			return
		}

		if y, err = conn.encode(encodeSET, append(append(set, byte(':')), conn.last...), nil, items.MapIndex(keys[i])); err != nil {
			return
		}

		k = k + x + y
	}

	return
}

/*
func (conn *Conn) encodeValue(value interface{}) (err error) {
	switch value := value.(type) {
	case []byte:
		err = conn.putBytes(value)
	case int:
		err = conn.putInt(int64(value))
	case int32:
		err = conn.putInt(int64(value))
	case int64:
		err = conn.putInt(value)
	case float32:
		err = conn.putFloat(float64(value))
	case float64:
		err = conn.putFloat(value)
	case bool:
		if value {
			err = conn.putString("1")
		} else {
			err = conn.putString("0")
		}
	case string:
		err = conn.putString(value)
	case nil:
		err = conn.putString("")
	default:
		err = fmt.Errorf("redis: unsupported type: %s", reflect.ValueOf(value).Type())
	}

	return
}

func (conn *Conn) encodePair(cmd string, key, value interface{}) (n int, err error) {
	_, err = conn.writer.WriteString(cmd)
	if err != nil {
		return
	}

	if err = conn.encodeValue(key); err != nil {
		return
	}

	if err = conn.encodeValue(value); err != nil {
		return
	}

	n = 1
	return
}

func (conn *Conn) encode(cmd string, key, value interface{}) (n int, err error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Struct:
		n, err = conn.marshalFields(key, "", v)
	case reflect.Slice, reflect.Array:
		n, err = conn.marshalItems(key, v)
	case reflect.Map:
		n, err = conn.marshalMapItems(key, v)
	case reflect.Interface, reflect.Ptr:
		if value != nil {
			n, err = conn.marshal(cmd, key, v.Elem())
		} else {
			n, err = conn.encodePair(cmd, key, nil)
		}
	default:
		n, err = conn.encodePair(cmd, key, value)
	}

	return
}

*/

/*
func (conn *Conn) marshal(key string, value reflect.Value) (err error) {
	switch value.Kind() {
	case reflect.Struct:
		err = conn.marshalFields(key, "", value)
	case reflect.Bool:
		err = conn.Put("SET", key, value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		err = conn.Put("SET", key, value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		err = conn.Put("SET", key, value.Uint())
	case reflect.Float32, reflect.Float64:
		err = conn.Put("SET", key, value.Float())
	case reflect.String:
		err = conn.Put("SET", key, value.String())
	case reflect.Slice, reflect.Array:
		err = conn.marshalItems(key, value)
	case reflect.Map:
		err = conn.marshalMapItems(key, value)
	case reflect.Interface, reflect.Ptr:
		if value != reflect.Zero(value.Type()) {
			err = conn.marshal(key, value.Elem())
		}
	default:
		err = fmt.Errorf("redis: unsupported type: %s", value.Type())
	}

	return
}

func (conn *Conn) marshalField(key, name string, value reflect.Value) (err error) {
	switch value.Kind() {
	case reflect.Struct:
		err = conn.marshalFields(key, name, value)
	case reflect.Bool:
		err = conn.Put("HSET", key, name, value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		err = conn.Put("HSET", key, name, value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		err = conn.Put("HSET", key, name, value.Uint())
	case reflect.Float32, reflect.Float64:
		err = conn.Put("HSET", key, name, value.Float())
	case reflect.String:
		err = conn.Put("HSET", key, name, value.String())
	case reflect.Slice, reflect.Array:
		err = conn.marshalItems(key+":"+name, value)
	case reflect.Map:
		err = conn.marshalMapItems(key+":"+name, value)
	case reflect.Interface, reflect.Ptr:
		if value != reflect.Zero(value.Type()) {
			err = conn.marshalField(key, name, value.Elem())
		}
	default:
		err = fmt.Errorf("redis: unsupported type: %s", value.Type())
	}

	return
}

func (conn *Conn) marshalItems(key string, items reflect.Value) (err error) {
	for i, n := 0, items.Len(); i < n && err == nil; i++ {
		err = conn.marshalItem(key, i, items.Index(i))
	}

	return
}

func (conn *Conn) marshalItem(key string, index int, value reflect.Value) (err error) {
	switch value.Kind() {
	case reflect.Struct:
		err = conn.marshalFields(fmt.Sprintf("%s:%d", key, index), "", value)
	case reflect.Bool:
		err = conn.Put("RPUSH", key, value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		err = conn.Put("RPUSH", key, value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		err = conn.Put("RPUSH", key, value.Uint())
	case reflect.Float32, reflect.Float64:
		err = conn.Put("RPUSH", key, value.Float())
	case reflect.String:
		err = conn.Put("RPUSH", key, value.String())
	case reflect.Slice, reflect.Array:
		for i, n := 0, value.Len(); i < n && err == nil; i++ {
			err = conn.mashalItem(fmt.Sprintf("%s:%d", key, index), value.Index(i))
		}
	case reflect.Map:
		keys := value.MapKeys()
		for i, n := 0, len(keys); i < n && err == nil; i++ {
			err = conn.marshalMapItem(key+":"+name, keys[i], value.MapIndex(keys[i]))
		}
	case reflect.Interface, reflect.Ptr:
		err = conn.marshalField(key, name, value.Elem())
	default:
		err = fmt.Errorf("redis: unsupported type: %s", value.Type())
	}

	return
}

func (conn *Conn) marshalMapItems(key string, items reflect.Value) (err error) {
	keys := items.MapKeys()
	for i, n := 0, len(keys); i < n && err == nil; i++ {
		err = conn.marshalMapItem(key, keys[i], items.MapIndex(keys[i]))
	}

	return
}

func (conn *Conn) marshalMapItem(key string, index, value reflect.Value) (err error) {
	text := ""
	switch index.Kind() {
	case reflect.Bool:
		err = conn.Put("SADD", key, value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		err = conn.Put("SADD", key, value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		err = conn.Put("SADD", key, value.Uint())
	case reflect.Float32, reflect.Float64:
		err = conn.Put("SADD", key, value.Float())
	case reflect.String:
		err = conn.Put("SADD", key, value.String())
	default:
		err = fmt.Errorf("redis: unsupported key type: %s", index.Type())
	}

	err = conn.marshalField(key)
	return
}
func (conn *Conn) putPrefix(cmd, key string) (err error) {
	_, err = conn.writer.WriteString(cmd)
	if err != nil {
		return
	}

	err = conn.putString(key)
	return
}

func (conn *Conn) marshalBytes(cmd, key string, value []byte) (n int, err error) {
	if err = conn.putPrefix(cmd, key); err != nil {
		return
	}

	if err = conn.putBytes(value); err == nil {
		n = 1
	}

	return
}

func (conn *Conn) marshalString(cmd, key, value string) (n int, err error) {
	if err = conn.putPrefix(cmd, key); err != nil {
		return
	}

	if err = conn.putString(value); err == nil {
		n = 1
	}

	return
}

func (conn *Conn) marshalInt(cmd, key string, value int64) (n int, err error) {
	if err = conn.putPrefix(cmd, key); err != nil {
		return
	}

	if err = conn.putInt(value); err == nil {
		n = 1
	}

	return
}

func (conn *Conn) marshalFloat(cmd, key string, value float64) (n int, err error) {
	if err = conn.putPrefix(cmd, key); err != nil {
		return
	}

	if err = conn.putFloat(value); err == nil {
		n = 1
	}

	return
}

*/
