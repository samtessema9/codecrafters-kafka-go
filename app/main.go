package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Server is listening on port 9092...")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
	
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	
	for {
		readBuf := make([]byte, 1024)
		conn.Read(readBuf)
	
		// create write buffer
		writeBuf := new(bytes.Buffer)
	
		parsedRequest := parseRequest(readBuf)

		var serializedResponse []byte
		if parsedRequest.headers.requestApiKey == 18 {
			// Construct ApiVersion v4 response 
			errorCode := checkVersion(parsedRequest.headers.requestApiVersion)
			apv := ApiVersionsResponse{}
			apv.errorCode = errorCode
			fmt.Printf("apv.errorCode: %v", apv.errorCode)
			apv.throttleTimeMS = int32(0)
			apv.apiKeys = append(apv.apiKeys, 
				ApiKey{
					apiKey: 18,
					minVersion: 0,
					maxVersion: 4,
				},
				ApiKey{
					apiKey: 75,
					minVersion: 0,
					maxVersion: 0,
				},
			)
			serializedResponse = apv.serialize()
		} else if parsedRequest.headers.requestApiKey == 75 {
			topicName := parseTopicName(parsedRequest.body)
			fmt.Printf("Parsed Name: (len)%v (value)%v", topicName.length, topicName.value)
			responseBody := DescribeTopicPartitionsResponse{
				topics: []Topic{
					{
						errorCode: 3,
						name: topicName,
						partitions: []Partition{},
					},
				},
			}
			serializedResponse = responseBody.serialize()
		}
		
		// write the message_size and correlation_id to the buffer in BigEndian binary format
		// binary.Write(writeBuf, binary.BigEndian, serializedResponse)

		// write headers
		binary.Write(writeBuf, binary.BigEndian, parsedRequest.headers.correlationId)

		if parsedRequest.headers.requestApiKey == 75 {
			binary.Write(writeBuf, binary.BigEndian, int8(0)) 
		}

		// write body
		binary.Write(writeBuf, binary.BigEndian, serializedResponse)
		fmt.Printf("\nSerializedResponse: % X", serializedResponse)
		fmt.Printf("\nFial writeBuf: % X", writeBuf.Bytes())

		// Add a byte for TAG_BUFFER
		// binary.Write(writeBuf, binary.BigEndian, int8(0)) 

		messageSize := int32(len(writeBuf.Bytes()))
		// fmt.Printf("\nMessage size: %v", messageSize)
		output := new(bytes.Buffer)
		binary.Write(output, binary.BigEndian, messageSize)
		binary.Write(output, binary.BigEndian, writeBuf.Bytes())

		// respond to the client with the value stored in our buffer
		conn.Write(output.Bytes())
	}
}


 