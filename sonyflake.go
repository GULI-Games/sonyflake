// Package sonyflake implements Sonyflake, a distributed unique ID generator inspired by Twitter's Snowflake.
//
// A Sonyflake ID is composed of
//     39 bits for time in units of 10 msec
//      8 bits for a sequence number
//     16 bits for a machine id
package sonyflake

import (
	"errors"
	"net"
	"sync"
	"time"
)

// These constants are the bit lengths of Sonyflake ID parts.
const (
	BitLenTime      = 31 // bit length of time
	BitLenSequence  = 8  // bit length of sequence number
	BitLenShardID   = 8
	BitLenMachineID = 63 - BitLenTime - BitLenSequence - BitLenShardID // bit length of machine id
)

// Settings configures Sonyflake:
//
// StartTime is the time since which the Sonyflake time is defined as the elapsed time.
// If StartTime is 0, the start time of the Sonyflake is set to "2014-09-01 00:00:00 +0000 UTC".
// If StartTime is ahead of the current time, Sonyflake is not created.
//
// MachineID returns the unique ID of the Sonyflake instance.
// If MachineID returns an error, Sonyflake is not created.
// If MachineID is nil, default MachineID is used.
// Default MachineID returns the lower 16 bits of the private IP address.
//
// CheckMachineID validates the uniqueness of the machine ID.
// If CheckMachineID returns false, Sonyflake is not created.
// If CheckMachineID is nil, no validation is done.
type Settings struct {
	StartTime      time.Time
	MachineID      func() (uint16, error)
	ShardID        func() (uint16, error)
	CheckMachineID func(uint16) bool
}

// Sonyflake is a distributed unique ID generator.
type Sonyflake struct {
	mutex       *sync.Mutex
	startTime   int64
	elapsedTime int64
	sequence    uint16
	shardID     uint16
	machineID   uint16
}
type SnoyflakeIDInfo struct {
	ID        int64
	Time      int32
	Sequence  uint8
	ShardID   uint8
	MachineID uint16
	MSB       int64
}

// NewSonyflake returns a new Sonyflake configured with the given Settings.
// NewSonyflake returns nil in the following cases:
// - Settings.StartTime is ahead of the current time.
// - Settings.MachineID returns an error.
// - Settings.CheckMachineID returns false.
func NewSonyflake(st Settings) *Sonyflake {
	sf := new(Sonyflake)
	sf.mutex = new(sync.Mutex)
	sf.sequence = uint16(1<<BitLenSequence - 1)

	if st.StartTime.After(time.Now()) {
		return nil
	}
	if st.StartTime.IsZero() {
		sf.startTime = toSonyflakeTime(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
	} else {
		sf.startTime = toSonyflakeTime(st.StartTime)
	}

	var err error

	if st.ShardID == nil {
		sf.shardID = 0
	} else {
		sf.machineID, err = st.ShardID()
	}

	if err != nil {
		return nil
	}

	if st.MachineID == nil {
		sf.machineID, err = lower16BitPrivateIP()
	} else {
		sf.machineID, err = st.MachineID()
	}

	if sf.machineID == 0 {
		sf.machineID, err = lower16BitPrivateIP()
	}

	if err != nil || (st.CheckMachineID != nil && !st.CheckMachineID(sf.machineID)) {
		return nil
	}

	return sf
}

// NextID generates a next unique ID.
// After the Sonyflake time overflows, NextID returns an error.
func (sf *Sonyflake) NextID() (uint64, error) {
	const maskSequence = uint16(1<<BitLenSequence - 1)

	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	current := currentElapsedTime(sf.startTime)
	if sf.elapsedTime < current {
		sf.elapsedTime = current
		sf.sequence = 0
	} else { // sf.elapsedTime >= current
		sf.sequence = (sf.sequence + 1) & maskSequence
		if sf.sequence == 0 {
			sf.elapsedTime++
			time.Sleep(sleepTime(toGeneralTime(sf.elapsedTime, sf.startTime)))
		}
	}

	return sf.toID()
}

func toSonyflakeTime(t time.Time) int64 {
	return t.UTC().Unix()
}

func currentElapsedTime(startTime int64) int64 {
	return toSonyflakeTime(time.Now()) - startTime
}
func toGeneralTime(snowflakeTime, startTime int64) time.Time {
	return time.Unix(snowflakeTime+startTime, 0)
}
func sleepTime(elapsedTime time.Time) time.Duration {
	rt := elapsedTime.Sub(time.Now().UTC())
	return rt
}

func (sf *Sonyflake) toID() (uint64, error) {
	if sf.elapsedTime >= 1<<BitLenTime {
		return 0, errors.New("over the time limit")
	}

	return uint64(sf.elapsedTime)<<(BitLenSequence+BitLenMachineID+BitLenShardID) |
		uint64(sf.sequence)<<(BitLenMachineID+BitLenShardID) |
		uint64(sf.shardID)<<BitLenShardID |
		uint64(sf.machineID), nil
}

func privateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if isPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}

func isPrivateIPv4(ip net.IP) bool {
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168)
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := privateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

// Decompose returns a set of Sonyflake ID parts.
func Decompose(id uint64) SnoyflakeIDInfo {
	const maskSequence = uint64((1<<BitLenSequence - 1) << (BitLenMachineID + BitLenShardID))
	const maskShardID = uint64(1<<BitLenShardID-1) << BitLenMachineID
	const maskMachineID = uint64(1<<BitLenMachineID - 1)

	msb := id >> 63
	creationTime := id >> (BitLenSequence + BitLenMachineID + BitLenShardID)
	sequence := id & maskSequence >> (BitLenMachineID + BitLenShardID)
	shardID := id & maskShardID >> BitLenMachineID
	machineID := id & maskMachineID
	return SnoyflakeIDInfo{
		ID:        int64(id),
		MSB:       int64(msb),
		Time:      int32(creationTime),
		Sequence:  uint8(sequence),
		ShardID:   uint8(shardID),
		MachineID: uint16(machineID),
	}
}
