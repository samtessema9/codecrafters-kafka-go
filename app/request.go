package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Request struct {
	messageSize int32
	headers     RequestHeadersV2
	body        RequestBody
}

type RequestHeadersV2 struct {
	requestApiKey     int16
	requestApiVersion int16
	correlationId     int32
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

func parseRequest(rawRequest []byte) (Request, error) {
	r := Request{}

	err := binary.Read(bytes.NewReader(rawRequest[:4]), binary.BigEndian, &r.messageSize)
	if err != nil {
		fmt.Println("Error converting bytes to int32:", err)
		return Request{}, err
	}

	err = binary.Read(bytes.NewReader(rawRequest[4:6]), binary.BigEndian, &r.headers.requestApiKey)
	if err != nil {
		fmt.Println("Error converting bytes to int32:", err)
		return Request{}, err
	}

	err = binary.Read(bytes.NewReader(rawRequest[6:8]), binary.BigEndian, &r.headers.requestApiVersion)
	if err != nil {
		fmt.Println("Error converting bytes to int32:", err)
		return Request{}, err
	}

	err = binary.Read(bytes.NewReader(rawRequest[8:12]), binary.BigEndian, &r.headers.correlationId)
	if err != nil {
		fmt.Println("Error converting bytes to int32:", err)
		return Request{}, err
	}

	return r, nil
}