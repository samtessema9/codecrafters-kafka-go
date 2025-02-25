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

	parsedTopics := parseClusterMetadataLogline()

	for {
		readBuf := make([]byte, 1024)
		conn.Read(readBuf)

		// create write buffer
		writeBuf := new(bytes.Buffer)

		// Parse the request
		parsedRequest := parseRequest(readBuf)

		var serializedResponse []byte

		requestApiKey := parsedRequest.headers.requestApiKey	
		switch requestApiKey {
		case 18:
			// Construct ApiVersion v4 response
			errorCode := checkVersion(parsedRequest.headers.requestApiVersion)
			apv := ApiVersionsResponse{}
			apv.errorCode = errorCode
			apv.apiKeys = append(apv.apiKeys,
				ApiKey{
					apiKey:     18,
					minVersion: 0,
					maxVersion: 4,
				},
				ApiKey{
					apiKey:     75,
					minVersion: 0,
					maxVersion: 0,
				},
				ApiKey{
					apiKey:     1,
					minVersion: 0,
					maxVersion: 16,
				},
			)
			serializedResponse = apv.serialize()
		case 75:
			// Construct DescribeTopicPartitions response
			topicNames := parseTopicNames(parsedRequest.body)

			relevantTopics := func(topics map[string]Topic, names []string) (filteredTopics []Topic) {
				for _, name := range names {
					topic, ok := topics[name]
					if ok {
						filteredTopics = append(filteredTopics, topic)
					}
				}

				return filteredTopics
			}(parsedTopics, topicNames)

			var responseBody DescribeTopicPartitionsResponse
			if len(relevantTopics) == 0 {
				responseBody = DescribeTopicPartitionsResponse{
					topics: []Topic{
						{
							errorCode: 3,
							name: CompactNullableString{
								length: int64(len(topicNames[0])),
								value:  topicNames[0],
							},
							topicID:    generateUUID(),
							partitions: []Partition{},
						},
					},
				}
			} else {
				responseBody = DescribeTopicPartitionsResponse{
					topics: relevantTopics,
				}
			}

			serializedResponse = responseBody.serialize()
			
		case 1: 
			fetchResponse := FetchResponseV16{}
			serializedResponse = fetchResponse.serialize()
		}	

		// write headers
		binary.Write(writeBuf, binary.BigEndian, parsedRequest.headers.correlationId)

		if parsedRequest.headers.requestApiKey == 75 {
			writeBuf.Write([]byte{0})
		}

		// write body
		writeBuf.Write(serializedResponse)

		messageSize := int32(len(writeBuf.Bytes()))

		// Construct final output
		output := new(bytes.Buffer)
		binary.Write(output, binary.BigEndian, messageSize)
		output.Write(writeBuf.Bytes())

		// respond to the client with the value stored in our buffer
		conn.Write(output.Bytes())
	}
}
