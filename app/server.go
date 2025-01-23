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

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	handleConn(conn)
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	
	readBuf := make([]byte, 1024)
	conn.Read(readBuf)
	fmt.Printf("Request: %v", readBuf)

	// create write buffer
	writeBuf := new(bytes.Buffer)

	messageSize := uint32(0)
	parsedRequest := parseRequest(readBuf)
	correlationId := parsedRequest.headers.correlationId
	apiVersion := parsedRequest.headers.requestApiVersion
	errorCode := checkVersion(apiVersion)
	
	// write the message_size and correlation_id to the buffer in BigEndian binary format
	binary.Write(writeBuf, binary.BigEndian, messageSize)
	binary.Write(writeBuf, binary.BigEndian, correlationId)
	binary.Write(writeBuf, binary.BigEndian, errorCode)

	// respond to the client with the value stored in our buffer
	conn.Write(writeBuf.Bytes())
}
