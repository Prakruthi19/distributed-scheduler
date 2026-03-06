package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"mini-k8s-scheduler/pkg/models"
)

// Worker represents a worker node that reports resources to master
type Worker struct {
	node    *models.Node
	running bool
}

// NewWorker creates a new worker node
func NewWorker(nodeID, nodeName string, totalResources models.Resource) *Worker {
	return &Worker{
		node:    models.NewNode(nodeID, nodeName, totalResources),
		running: false,
	}
}

// GetSystemResources simulates reading actual system resources
// In real implementation, this would use syscall/system packages
func (w *Worker) GetSystemResources() models.Resource {
	// Simulate some system resource fluctuation
	cpuVariance := rand.Intn(500) - 250
	memVariance := rand.Intn(512) - 256

	available := models.Resource{
		CPU:    w.node.Available.CPU + int64(cpuVariance),
		Memory: w.node.Available.Memory + int64(memVariance),
		Disk:   w.node.Available.Disk,
	}

	// Ensure we don't report negative resources
	if available.CPU < 0 {
		available.CPU = 0
	}
	if available.Memory < 0 {
		available.Memory = 0
	}

	return available
}

// Start begins the worker's heartbeat loop
func (w *Worker) Start() {
	w.running = true
	log.Printf("[WORKER %s] Starting...\n", w.node.Name)
	go w.heartbeatLoop()
	go w.taskExecutionLoop()
}

// Stop halts the worker
func (w *Worker) Stop() {
	w.running = false
	log.Printf("[WORKER %s] Stopped\n", w.node.Name)
}

// heartbeatLoop sends periodic heartbeats to update resource status
func (w *Worker) heartbeatLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for w.running {
		select {
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

// sendHeartbeat reports the worker's health status
func (w *Worker) sendHeartbeat() {
	w.node.LastSeen = time.Now()
	w.node.Available = w.GetSystemResources()
	
	log.Printf("[WORKER %s] Heartbeat - CPU: %d/%d | Memory: %dMB/%dMB | Tasks: %d\n",
		w.node.Name,
		w.node.Total.CPU-w.node.Available.CPU, w.node.Total.CPU,
		w.node.Total.Memory-w.node.Available.Memory, w.node.Total.Memory,
		len(w.node.Tasks),
	)
}

// taskExecutionLoop simulates running assigned tasks
func (w *Worker) taskExecutionLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for w.running {
		select {
		case <-ticker.C:
			w.executeTasks()
		}
	}
}

// executeTasks simulates task execution and resource usage
func (w *Worker) executeTasks() {
	if len(w.node.Tasks) == 0 {
		return
	}

	log.Printf("[WORKER %s] Executing %d task(s)...\n", w.node.Name, len(w.node.Tasks))

	// Simulate tasks using resources
	for i, taskID := range w.node.Tasks {
		log.Printf("  Task %d: %s\n", i+1, taskID)
	}
}

// AssignTask assigns a task to this worker
func (w *Worker) AssignTask(task *models.Task) bool {
	if !w.node.CanFit(task) {
		log.Printf("[WORKER %s] Cannot fit task %s (insufficient resources)\n", w.node.Name, task.Name)
		return false
	}

	w.node.AllocateResources(task)
	log.Printf("[WORKER %s] ✓ Task assigned: %s\n", w.node.Name, task.Name)
	return true
}

// CompleteTask marks a task as completed and frees resources
func (w *Worker) CompleteTask(taskID string) {
	// Find and remove the task
	for i, tid := range w.node.Tasks {
		if tid == taskID {
			log.Printf("[WORKER %s] Task %s completed\n", w.node.Name, taskID)
			
			// In a real scenario, we'd track the task object
			// For now, just remove from the list
			w.node.Tasks = append(w.node.Tasks[:i], w.node.Tasks[i+1:]...)
			break
		}
	}
}

// GetStatus returns the worker's current status
func (w *Worker) GetStatus() string {
	return fmt.Sprintf(`
╔════════════════════════════════════════════╗
║        WORKER STATUS - %s             ║
╚════════════════════════════════════════════╝

📍 Node Information:
  • ID: %s
  • Name: %s
  • Healthy: %v
  • Last Seen: %s

💾 Resource Status:
  • CPU: %d/%d millicores (%.2f%% used)
  • Memory: %dMB/%dMB (%.2f%% used)
  • Disk: %dMB/%dMB

📋 Running Tasks: %d
`,
		w.node.Name,
		w.node.ID,
		w.node.Name,
		w.node.Healthy,
		time.Since(w.node.LastSeen),
		w.node.Total.CPU-w.node.Available.CPU,
		w.node.Total.CPU,
		(1 - float64(w.node.Available.CPU)/float64(w.node.Total.CPU)) * 100,
		w.node.Total.Memory-w.node.Available.Memory,
		w.node.Total.Memory,
		(1 - float64(w.node.Available.Memory)/float64(w.node.Total.Memory)) * 100,
		w.node.Total.Disk-w.node.Available.Disk,
		w.node.Total.Disk,
		len(w.node.Tasks),
	)
}

// ===== MAIN SIMULATION =====

func main() {
	rand.Seed(time.Now().UnixNano())

	// Create a worker node (simulating a machine with 4 CPUs, 8GB RAM, 50GB disk)
	worker := NewWorker("node-1", "Worker-1", models.NewResource(4000, 8192, 50000))
	worker.Start()

	// Simulate assigning some tasks
	task1 := models.NewTask("task-1", "Processing Job", models.NewResource(1000, 2048, 5000))
	task2 := models.NewTask("task-2", "ML Training", models.NewResource(2000, 4096, 20000))
	task3 := models.NewTask("task-3", "API Server", models.NewResource(500, 1024, 2000))

	worker.AssignTask(task1)
	worker.AssignTask(task2)
	worker.AssignTask(task3)

	// Let worker run
	time.Sleep(5 * time.Second)

	// Print status
	fmt.Println(worker.GetStatus())

	// Complete a task
	worker.CompleteTask("task-1")
	time.Sleep(1 * time.Second)

	fmt.Println(worker.GetStatus())

	// Cleanup
	worker.Stop()
}