package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Request struct {
	messageSize uint32
	headers     RequestHeadersV2
	body        []byte
}

type RequestHeadersV2 struct {
	requestApiKey     uint16
	requestApiVersion int16
	correlationId     uint32
	clientId          NullableString
	tagBuffer         int8 // TODO: This assumes an empty TAG_BUFFER. This is true for now but needs to be adressed at some point.
}

type RequestBody struct {
	// TODO
}

type ComapactArray struct {
	// TODO
}

func parseRequest(rawRequest []byte) Request {
	r := Request{}

	// Parse Headers from request
	r.messageSize = binary.BigEndian.Uint32(rawRequest[:4])
	r.headers.requestApiKey = binary.BigEndian.Uint16(rawRequest[4:6])
	r.headers.requestApiVersion = int16(binary.BigEndian.Uint16(rawRequest[6:8]))
	r.headers.correlationId = binary.BigEndian.Uint32(rawRequest[8:12])
	r.headers.clientId = parseNullableString(rawRequest[12:])

	// Calculate the offset and jump to the TAG_BUFFER that comes after the clientId
	offset := r.headers.clientId.length + 2
	r.headers.tagBuffer = int8(rawRequest[12+offset])

	// Parse Body from request
	r.body = rawRequest[12+offset+1:]

	return r
}

func parseTopicNames(buf []byte) []string {
	bufReader := bytes.NewBuffer(buf)

	lenOfTopics, err := binary.ReadUvarint(bufReader)
	if err != nil {
		fmt.Errorf("Error parsing len of topics (in: parseTopicName): ", err)
	}
		
	names := []string{}
	for range lenOfTopics - 1 {
		lenOfName := int8(bufReader.Next(1)[0])

		name := string(bufReader.Next(int(lenOfName) - 1))
		names = append(names, name)

		// TAG_BUFFER
		_ = bufReader.Next(1)
	}

	return names
}
