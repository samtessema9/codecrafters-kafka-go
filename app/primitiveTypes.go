package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"

	"golang.org/x/exp/constraints"
)

type NullableString struct {
	length int16
	value  string
}

func parseNullableString(source []byte) NullableString {
	length := int16(binary.BigEndian.Uint16(source[0:2]))

	if length == -1 {
		return NullableString{
			length: 0,
			value: "",
		}
	}

	value := string(source[2 : length+2])

	return NullableString{
		length: int16(length),
		value:  value,
	}
}

type CompactNullableString struct {
	length int64
	value string
} 

func (cns CompactNullableString) serialize() []byte {
	buf := new(bytes.Buffer)

	buf.Write([]byte{byte(len(cns.value) + 1)})
	buf.WriteString(cns.value)

	return buf.Bytes()

	// buf := make([]byte, 0)

	// buf = append(buf, byte(len(cns.value) + 1))
	// buf = append(buf, []byte(cns.value)...)

	// return buf
}

func parseCompactNullableString(source []byte) CompactNullableString {
	// TODO: we are assuming length of name is always 1. Should be parsed as a varint?
	length := int(source[0])
	value := string(source[1 : length])

	if length == 0 {
		return CompactNullableString{
			length: 0,
			value: "",
		}
	}

	return CompactNullableString{
		length: int64(length),
		value:  value,
	}
}

type CompactString struct {
	length int64
	value string 
}

func (cs CompactString) serialize() []byte {
	buf := new(bytes.Buffer)

    tmp := make([]byte, binary.MaxVarintLen64)
    binary.PutVarint(tmp, cs.length)
	binary.Write(buf, binary.BigEndian, tmp)

	if _, err := buf.WriteString(cs.value); err != nil {
        panic(err)
    }

	return buf.Bytes()
}

func parseCompactString(source []byte) CompactString {
	length, offset := binary.Varint(source)
	value := string(source[offset : length+1])

	return CompactString{
		length: length,
		value:  value,
	}
}

type UUID struct {
	uuid string
}

func generateUUID() UUID {
	// TODO: This is hardcode. We should generate an actual UUID here.
	return UUID{
		uuid: "00000000-0000-0000-0000-000000000000",
	}
}

func (uuid UUID) serialize() []byte {
	uuidWithoutDashes := strings.ReplaceAll(uuid.uuid, "-", "")

	b, err := hex.DecodeString(uuidWithoutDashes)
	if err != nil {
		panic(err)
	}

	return b
}

type TagBuffer struct {
	// TODO
}

func boolToByte(x bool) byte {
    if x {
        return 1
    }
    return 0
}

// Define a constraint that includes all integer types
type Integer interface {
	constraints.Integer
}

func serializeArray[T Integer](arr []T) []byte {
	buf := new(bytes.Buffer)

	// TODO: we're assuming len will be 1 byte. make this dynamic.
	lenOfArr := int8(len(arr) + 1)
	binary.Write(buf, binary.BigEndian, lenOfArr)

	for _, num := range arr {
		binary.Write(buf, binary.BigEndian, num)
	}

	return buf.Bytes()
}
