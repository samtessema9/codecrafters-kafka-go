package main

import (
	"bytes"
	"encoding/binary"
)

// DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER
//   topics => name TAG_BUFFER
//     name => COMPACT_STRING
//   response_partition_limit => INT32
//   cursor => topic_name partition_index TAG_BUFFER
//     topic_name => COMPACT_STRING
//     partition_index => INT32
type DescribeTopicPartitionsRequest struct {
	topics []Topic 
	responsePartitionLimit int32 
	cursor Cursor
	tagBuffer int8
}

type DescribeTopicPartitionsResponse struct {
	throttleTimeMS int32
	topics         []Topic
	nextCursor     Cursor
	tagBuffer      int8
}

type Topic struct {
	errorCode                 int16
	name                      CompactNullableString
	topicID                   UUID
	isInternal                bool
	partitions                []Partition
	topicAuthorizedOperations int32
	tagBuffer                 int8
}

type Cursor struct {
	topicName      CompactString
	partitionIndex int32
	tagBuffer      int8
}

func (c Cursor) serialize() []byte {
	buf := new(bytes.Buffer) 

	if c.topicName.value == "" && c.partitionIndex == 0 {
		binary.Write(buf, binary.BigEndian, uint8(255))	
		return buf.Bytes()
	}

    binary.Write(buf, binary.BigEndian, c.topicName.serialize())	
	binary.Write(buf, binary.BigEndian, c.partitionIndex)
	binary.Write(buf, binary.BigEndian, c.tagBuffer)

	return buf.Bytes()
}

type Partition struct {
	errorCode              int16
	partitionIndex         int32
	leaderId               int32
	leaderEpoch            int32
	replicaNodes           []int32
	isrNodes               []int32
	eligibleLeaderReplicas []int32
	lastKnownElr           []int32
	offlineReplicas        []int32
	tagBuffer              int8
}

func (dtpr DescribeTopicPartitionsRequest) parse(buf []byte) {
	// lenOfTopics, n := binary.Varint(buf)
    // if n <= 0 {
    //     fmt.Println("Error decoding varint")
    //     return
    // }

	// TODO: This assumes there's only one topic in the array. Use above apprioach to determine actual length of array.
	
}

func (dtpr DescribeTopicPartitionsResponse) serialize() []byte {
	buf := new(bytes.Buffer)

	// Throttle time (ms)
	binary.Write(buf, binary.BigEndian, uint32(dtpr.throttleTimeMS))

	// Topic Length
	lenOfTopics := int8(len(dtpr.topics) + 1)
	binary.Write(buf, binary.BigEndian, lenOfTopics)

	for _, topic := range dtpr.topics {
		serializedTopic := topic.serialize()
		binary.Write(buf, binary.BigEndian, serializedTopic)
	}

	// Next Cursor
	binary.Write(buf, binary.BigEndian, dtpr.nextCursor.serialize())

	// TAG_BUFFER
	buf.Write([]byte{0})

	return buf.Bytes()
}

func (topic Topic) serialize() []byte {
	buf := new(bytes.Buffer)

	// ErrorCode
	binary.Write(buf, binary.BigEndian, topic.errorCode)

	// Topic name
	buf.Write(topic.name.serialize())

	// Topic ID
	uuid := generateUUID()
	binary.Write(buf, binary.BigEndian, uuid.serialize())

	// isInternal
	buf.WriteByte(boolToByte(topic.isInternal))

	// Partition Length
	buf.Write([]byte{byte(len(topic.partitions) + 1)})

	// Authorized Operations
	binary.Write(buf, binary.BigEndian, topic.topicAuthorizedOperations)

	// TAG_BUFFER
	buf.Write([]byte{0})

	return buf.Bytes()
}

