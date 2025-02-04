package main

import (
	"bytes"
	"encoding/binary"
)

type ApiVersionsResponse struct {
	errorCode      int16
	apiKeys        []ApiKey
	throttleTimeMS int32
}

type ApiKey struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}

func (avr ApiVersionsResponse) serialize() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, avr.errorCode)

	// https://forum.codecrafters.io/t/question-about-handle-apiversions-requests-stage/1743
	// keyLenBuffer := make([]byte, binary.MaxVarintLen64)
	lenOfApiKeys := int8(len(avr.apiKeys) + 1)

	binary.Write(buf, binary.BigEndian, lenOfApiKeys)

	for _, apiKey := range avr.apiKeys {
		serializedApiKey := apiKey.serialize()
		binary.Write(buf, binary.BigEndian, serializedApiKey)

		taggedFields := int8(0)
		binary.Write(buf, binary.BigEndian, taggedFields)
	}

	binary.Write(buf, binary.BigEndian, avr.throttleTimeMS)

	// TAG_BUFFER
	binary.Write(buf, binary.BigEndian, int8(0))

	return buf.Bytes()
}

func (ak ApiKey) serialize() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, ak.apiKey)
	binary.Write(buf, binary.BigEndian, ak.minVersion)
	binary.Write(buf, binary.BigEndian, ak.maxVersion)

	return buf.Bytes()
}

func checkVersion(version int16) int16 {
	if version >= 0 && version <= 4 {
		return int16(0)
	}

	return int16(35)
}
