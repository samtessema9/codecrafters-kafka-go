package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// read and ignore clients message
	readBuf := []byte{}
	_, _ = conn.Read(readBuf)

	// create write buffer
	writeBuf := new(bytes.Buffer)

	messageSize := uint32(0)
	correlationId := uint32(7)
	
	// write the message_size and correlation_id to the buffer in BigEndian binary format
	binary.Write(writeBuf, binary.BigEndian, messageSize)
	binary.Write(writeBuf, binary.BigEndian, correlationId)

	fmt.Printf("buf: % X", writeBuf.Bytes())

	// respond to the client with the value stored in our buffer
	conn.Write(writeBuf.Bytes())
}
