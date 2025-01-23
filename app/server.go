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
	
		// respond to the client with the value stored in our buffer
		conn.Write(writeBuf.Bytes())
	}
}