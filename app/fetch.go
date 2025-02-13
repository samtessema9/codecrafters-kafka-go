package main

import (
	"bytes"
	"encoding/binary"
)

// Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
//   throttle_time_ms => INT32
//   error_code => INT16
//   session_id => INT32
//   responses => topic_id [partitions] TAG_BUFFER
//     topic_id => UUID
//     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
//       partition_index => INT32
//       error_code => INT16
//       high_watermark => INT64
//       last_stable_offset => INT64
//       log_start_offset => INT64
//       aborted_transactions => producer_id first_offset TAG_BUFFER
//         producer_id => INT64
//         first_offset => INT64
//       preferred_read_replica => INT32
//       records => COMPACT_RECORDS
type FetchResponseV16 struct {
	throttleTimeMS int32
	errorCode int16 
	sessionID int32 
	responses []Response
	TagBuffer int8
}

func (fr FetchResponseV16) serialize() []byte {
	buf := new(bytes.Buffer) 

	// Throttle Time
	binary.Write(buf, binary.BigEndian, fr.throttleTimeMS)

	// Error Code
	binary.Write(buf, binary.BigEndian, fr.errorCode)

	// Session ID
	binary.Write(buf, binary.BigEndian, fr.sessionID)

	// Responses 
	binary.Write(buf, binary.BigEndian, int8(len(fr.responses) + 1))
	
	for _, response := range fr.responses {
		binary.Write(buf, binary.BigEndian, response.serialize())
	}

	// TAG_BUFFER
	binary.Write(buf, binary.BigEndian, fr.TagBuffer)

	return buf.Bytes()
}

type Response struct {
	topicID UUID 
	partitions []FetchPartition
	tagBuffer int8
}

func (r Response) serialize() []byte {
	// TODO
	return nil
}

type FetchPartition struct {
	partitionIndex int32 
	errorCode int16 
	highWatermark int64 
	lastStableOffset int64 
	logStartOffset int64 
	abortedTransactions []AbortedTransaction
	preferredReadReplica int32 
	records CompactRecords
}

func (fp FetchPartition) serialize() []byte {
	// TODO
	return nil
}

type AbortedTransaction struct {
	producerID int64 
	firstOffset int64
}

func (at AbortedTransaction) serialize() []byte {
	// TODO 
	return nil
}

type CompactRecords struct {
	// TODO
}

func (cr CompactRecords) serialize() []byte {
	// TODO 
	return nil
}