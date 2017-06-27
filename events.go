package main

import "time"

const (
	StatusUpdateEvent = "status_update_event"
)

// This package is intentionally left incomplete. It can be extended with an exhaustive list in the future
// if more complicated strategies are implemented or if there is a more generic use case for typed
// Marathon events

type StatusUpdate struct {
	EventType   string    `json:"eventType"`
	Timestamp   time.Time `json:"timestamp"`
	SlaveID     string    `json:"slaveId"`
	TaskID      string    `json:"taskId"`
	TaskStatus  string    `json:"taskStatus"`
	Message     string    `json:"message"`
	AppID       string    `json:"appId"`
	Host        string    `json:"host"`
	IPAddresses []struct {
		IPAddress string `json:"ipAddress"`
		Protocol  string `json:"protocol"`
	} `json:"ipAddresses"`
	Ports   []int     `json:"ports"`
	Version time.Time `json:"version"`
}
