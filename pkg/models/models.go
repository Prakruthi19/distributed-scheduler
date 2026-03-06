package models

import (
	"time"
)

// Resource represents CPU, memory, and disk resources
type Resource struct {
	CPU    int64 // in millicores (1000 = 1 CPU)
	Memory int64 // in MB
	Disk   int64 // in MB
}

// Task represents a unit of work to be scheduled
type Task struct {
	ID           string
	Name         string
	Required     Resource
	CreatedAt    time.Time
	Status       string // "pending", "running", "completed", "failed"
	AssignedNode string
}

// Node represents a worker machine in the cluster
type Node struct {
	ID        string
	Name      string
	Available Resource // Resources available for allocation
	Total     Resource // Total resources on the node
	Tasks     []string // Task IDs running on this node
	Healthy   bool
	LastSeen  time.Time
}

// SchedulingResult represents the outcome of scheduling a task
type SchedulingResult struct {
	TaskID   string
	NodeID   string
	Success  bool
	Reason   string
	Score    float64 // How good of a fit this node is
}

// ClusterState represents the current state of all nodes
type ClusterState struct {
	Nodes map[string]*Node
}

// NewResource creates a new Resource
func NewResource(cpu, memory, disk int64) Resource {
	return Resource{
		CPU:    cpu,
		Memory: memory,
		Disk:   disk,
	}
}

// NewTask creates a new Task
func NewTask(id, name string, required Resource) *Task {
	return &Task{
		ID:        id,
		Name:      name,
		Required:  required,
		CreatedAt: time.Now(),
		Status:    "pending",
	}
}

// NewNode creates a new Node
func NewNode(id, name string, total Resource) *Node {
	return &Node{
		ID:        id,
		Name:      name,
		Available: total,
		Total:     total,
		Tasks:     []string{},
		Healthy:   true,
		LastSeen:  time.Now(),
	}
}

// CanFit checks if a task can fit on a node
func (n *Node) CanFit(task *Task) bool {
	if !n.Healthy {
		return false
	}
	
	return n.Available.CPU >= task.Required.CPU &&
		n.Available.Memory >= task.Required.Memory &&
		n.Available.Disk >= task.Required.Disk
}

// AllocateResources reserves resources on a node
func (n *Node) AllocateResources(task *Task) {
	n.Available.CPU -= task.Required.CPU
	n.Available.Memory -= task.Required.Memory
	n.Available.Disk -= task.Required.Disk
	n.Tasks = append(n.Tasks, task.ID)
}

// ReleaseResources frees up resources on a node
func (n *Node) ReleaseResources(task *Task) {
	n.Available.CPU += task.Required.CPU
	n.Available.Memory += task.Required.Memory
	n.Available.Disk += task.Required.Disk
	
	// Remove task from list
	for i, tid := range n.Tasks {
		if tid == task.ID {
			n.Tasks = append(n.Tasks[:i], n.Tasks[i+1:]...)
			break
		}
	}
}

// GetUtilization returns the percentage of resources used
func (n *Node) GetUtilization() float64 {
	if n.Total.CPU == 0 || n.Total.Memory == 0 {
		return 0
	}
	
	cpuUsed := float64(n.Total.CPU - n.Available.CPU)
	memUsed := float64(n.Total.Memory - n.Available.Memory)
	
	cpuPercent := (cpuUsed / float64(n.Total.CPU)) * 100
	memPercent := (memUsed / float64(n.Total.Memory)) * 100
	
	// Average of CPU and Memory utilization
	return (cpuPercent + memPercent) / 2
}