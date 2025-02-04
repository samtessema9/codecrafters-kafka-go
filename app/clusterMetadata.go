package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/gofrs/uuid/v5"
)

type ClusterMetadataLogline struct {

}

func parseClusterMetadataLogline(logLine []byte) (topics []Topic) {
	data, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		fmt.Errorf("Error parsing metadata logfile: %v", err)
	}

	buf := bytes.NewBuffer(data) 

	for {
		/*
			BatchOffset: (8 bytes)
			BatchLength: (4 bytes)
			PartitionLeaderEpoch: (4 bytes)
			MagicByte: (1 byte)
			RecordCRC32C: (4 bytes)
			Attribute: (2 bytes)
			LastOffSetDelta: (4 bytes)
			BaseTimestamp: (8 bytes)
			MaxTimestamp: (8 bytes)
			ProducerID: (8 bytes)
			ProducerEpoch: (2 bytes)
			BaseSequence: (4 bytes)
		*/

		_ = buf.Next(57)

		var records int32 
		_ = binary.Read(buf, binary.BigEndian, &records)

		var partitions []Partition
		var topics map[UUID]Topic = map[UUID]Topic{}

		for range records {
			/* 
			    - Length (signed varint)
				- Attributes (1 bytes: Big Endian)
				- Timestamp Delta (1 byte: signed int)
				- Offset Delta (signed varint)
				- Key Length (signed varint)
				- Key (Doesn't exist)
				- Value Length  (signed varint)
			*/
			_, _ = binary.ReadVarint(buf)   // Length
			_ = buf.Next(1)				    // Attributes
			_ = buf.Next(1)					// Timestamp Delta
			_, _ = binary.ReadVarint(buf)	// Offset Delta
			_, _ = binary.ReadVarint(buf)	// Key Length 

			valueLength, err := binary.ReadVarint(buf)   
			if err != nil {
				fmt.Errorf("Error parsing value length from logs: %v", err)
			}

			valueBuf := bytes.NewBuffer(make([]byte, valueLength))
			_ = binary.Read(buf, binary.BigEndian, valueBuf) 

			_ = valueBuf.Next(1) // Discard Frame version (1 byte)

			var recordType int8
			_ = binary.Read(valueBuf, binary.BigEndian, &recordType)

			switch recordType {
			case 2: // Topic Record 
				var topic Topic

				_ = valueBuf.Next(1) // Discard version (1 byte)

				lengthOfName, err := binary.ReadUvarint(valueBuf)
				if err != nil {
					fmt.Errorf("Error reading Length of Topic name: %v", err)
				}
				name := make([]byte, lengthOfName)
				_ = binary.Read(valueBuf, binary.BigEndian, name)
				topic.name = CompactNullableString{
					length: int64(lengthOfName),
					value: string(name),
				}
				
				uuid, err := uuid.FromBytes(valueBuf.Next(16))
				if err != nil {
					fmt.Errorf("Error parsing uuid: %v", err)
				}
				topic.topicID = UUID{
					uuid: uuid.String(),
				}

				_, _ = binary.ReadUvarint(valueBuf)  // Discard Tagged Fields Count
				
				topics[topic.topicID] = topic

			case 3: // Partition Record
				var partition Partition

				_ = valueBuf.Next(1) // Discard version (1 byte)

				_ = binary.Read(valueBuf, binary.BigEndian, &partition.partitionIndex)

				uuid, err := uuid.FromBytes(valueBuf.Next(16))
				if err != nil {
					fmt.Errorf("Error parsing uuid: %v", err)
				}
				partition.topicID = UUID{
					uuid: uuid.String(),
				}

				lenOfReplicaArr, err := binary.ReadUvarint(valueBuf)
				if err != nil {
					fmt.Errorf("Error Parsing Replica Array Length: %v", err)
				}
				partition.replicaNodes = make([]int32, lenOfReplicaArr - 1)
				_ = binary.Read(valueBuf, binary.BigEndian, partition.replicaNodes)

				lenOfISRReplicaArr, err := binary.ReadUvarint(valueBuf)
				if err != nil {
					fmt.Errorf("Error Parsing In Sync Replica Array Length: %v", err)
				}
				partition.isrNodes = make([]int32, lenOfISRReplicaArr - 1)
				_ = binary.Read(valueBuf, binary.BigEndian, partition.isrNodes)

				_, _ = binary.ReadUvarint(valueBuf)  // Discard - Length of Removing Replicas array
				_, _ = binary.ReadUvarint(valueBuf)  // Discard - Length of Adding Replicas array

				_ = binary.Read(valueBuf, binary.BigEndian, &partition.leaderId)
				_ = binary.Read(valueBuf, binary.BigEndian, &partition.leaderEpoch)

				_ = valueBuf.Next(4)  // Discard Partition Epoch

				lenOfDirArr, err := binary.ReadUvarint(valueBuf)  // Length of Directrories Array
				if err != nil {
					fmt.Errorf("Error Parsing Length of Directrories Array: %v", err)
				}
				_ = valueBuf.Next((int(lenOfDirArr - 1)) * 16)  // Discard the contents of the Directories Array

				_ = valueBuf.Next(1)  // Discard tagged fields - this assumes tagged fields is 0

				partitions = append(partitions, partition)

			default:
				// remove Headers Array Count (unsigned varint)
				_, _ = binary.ReadUvarint(buf)
				continue
			}

			// remove Headers Array Count (unsigned varint)
			_, _ = binary.ReadUvarint(buf)
		}

		return coorelateTopicsAndPartitions(topics, partitions) 
	}
}

func coorelateTopicsAndPartitions(topicsMap map[UUID]Topic, partitions []Partition) []Topic {
	for _, partition := range partitions {
		topic, ok := topicsMap[partition.topicID]
		if ok {
			topic.partitions = append(topic.partitions, partition)
		}
	}

	topics := make([]Topic, len(topicsMap))

	for _, topic := range topicsMap {
		topics = append(topics, topic)
	}

	return topics
}
