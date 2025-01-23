package main

import (
	"encoding/binary"
)

type Request struct {
	messageSize uint32
	headers     RequestHeadersV2
	body        RequestBody
}

type RequestHeadersV2 struct {
	requestApiKey     uint16
	requestApiVersion uint16
	correlationId     uint32
	clientId          NullableString
	tagBuffer         ComapactArray
}

type RequestBody struct {
	// TODO
}

type NullableString struct {
	// TODO
}

type ComapactArray struct {
	// TODO
}

func parseRequest(rawRequest []byte) Request {
	r := Request{}

	r.messageSize = binary.BigEndian.Uint32(rawRequest[:4])
	r.headers.requestApiKey = binary.BigEndian.Uint16(rawRequest[4:6])
	r.headers.requestApiVersion = binary.BigEndian.Uint16(rawRequest[6:8])
	r.headers.correlationId = binary.BigEndian.Uint32(rawRequest[8:12])

	return r
}