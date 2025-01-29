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
	writer := new(bytes.Buffer)
	// buf := make([]byte, 0)
	// writer := bytes.NewBuffer(buf)

	binary.Write(writer, binary.BigEndian, uint32(dtpr.throttleTimeMS))
	// buf = binary.BigEndian.AppendUint32(buf, uint32(dtpr.throttleTimeMS))
	// buf =append(buf, byte(uint32(dtpr.throttleTimeMS)))

	lenOfTopics := int8(len(dtpr.topics) + 1)
	binary.Write(writer, binary.BigEndian, lenOfTopics)
	// buf = append(buf, uint8(len(dtpr.topics)+1))

	for _, topic := range dtpr.topics {
		serializedTopic := topic.serialize()
		binary.Write(writer, binary.BigEndian, serializedTopic)
		// buf = append(buf, serializedTopic...)
	}

	binary.Write(writer, binary.BigEndian, dtpr.nextCursor.serialize())
	// buf = append(buf, dtpr.nextCursor.serialize()...)

	// TAG_BUFFER
	binary.Write(writer, binary.BigEndian, uint8(0))
	// buf = append(buf, uint8(0))

	// return buf.Bytes()
	return writer.Bytes()
}

func (topic Topic) serialize() []byte {
	buf := new(bytes.Buffer)
	// b := make([]byte, 0)
	// buf := bytes.NewBuffer(b)

	binary.Write(buf, binary.BigEndian, topic.errorCode)
	buf.Write(topic.name.serialize())
	// buf = binary.BigEndian.AppendUint16(buf, uint16(topic.errorCode))
	// buf = append(buf, byte(len(topic.name.value) + 1))
	// buf = append(buf, []byte(topic.name.value)...)

	uuid := generateUUID()
	binary.Write(buf, binary.BigEndian, uuid.serialize())
	// buf = append(buf, uuid.serialize()...)
	// binary.Write(buf, binary.BigEndian, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,})

	// binary.Write(buf, binary.BigEndian, topic.isInternal)
	buf.Write([]byte{1})
	// buf = append(buf, byte(1))

	// binary.Write(buf, binary.BigEndian, int8(len(topic.partitions) + 1))
	// buf = append(buf, byte(len(topic.partitions) + 1) )
	buf.Write([]byte{byte(len(topic.partitions) + 1)})

	binary.Write(buf, binary.BigEndian, topic.topicAuthorizedOperations)
	// binary.Write(buf, binary.BigEndian, []byte{0x00, 0x00, 0x0d, 0xf8})
	// buf = binary.BigEndian.AppendUint32(buf, uint32(topic.topicAuthorizedOperations))

	// TAG_BUFFER
	// binary.Write(buf, binary.BigEndian, int8(0))
	// buf = append(buf, uint8(0))
	buf.Write([]byte{0})

	// return buf.Bytes()
	return buf.Bytes()
}

