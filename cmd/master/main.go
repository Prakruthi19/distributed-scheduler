package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"mini-k8s-scheduler/internal/scheduler"
	"mini-k8s-scheduler/pkg/models"
)

// Master represents the master node that orchestrates task scheduling
type Master struct {
	clusterState *models.ClusterState
	scheduler    *scheduler.BinPackingScheduler
	taskQueue    chan *models.Task
	mu           sync.RWMutex
	done         chan bool
}

// NewMaster creates a new master instance
func NewMaster() *Master {
	return &Master{
		clusterState: &models.ClusterState{
			Nodes: make(map[string]*models.Node),
		},
		scheduler: scheduler.NewBinPackingScheduler("best-fit"),
		taskQueue: make(chan *models.Task, 100),
		done:      make(chan bool),
	}
}

// RegisterNode adds a worker node to the cluster
func (m *Master) RegisterNode(node *models.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.clusterState.Nodes[node.ID] = node
	log.Printf("[MASTER] Node registered: %s (CPU: %d, Memory: %dMB)\n", node.Name, node.Total.CPU, node.Total.Memory)
}

// UnregisterNode removes a worker node from the cluster
func (m *Master) UnregisterNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, exists := m.clusterState.Nodes[nodeID]; exists {
		// Return all allocated resources
		for _, taskID := range node.Tasks {
			log.Printf("[MASTER] Task %s was running on %s, marking as orphaned\n", taskID, node.Name)
		}
		delete(m.clusterState.Nodes, nodeID)
		log.Printf("[MASTER] Node unregistered: %s\n", node.Name)
	}
}

// SubmitTask adds a task to the scheduling queue
func (m *Master) SubmitTask(task *models.Task) {
	m.taskQueue <- task
	log.Printf("[MASTER] Task submitted: %s (CPU: %d, Memory: %dMB)\n", task.Name, task.Required.CPU, task.Required.Memory)
}

// Start begins the master's scheduling loop
func (m *Master) Start() {
	log.Println("[MASTER] Starting scheduler...")
	go m.schedulingLoop()
	go m.healthCheckLoop()
}

// Stop halts the master
func (m *Master) Stop() {
	m.done <- true
	close(m.taskQueue)
	log.Println("[MASTER] Stopped")
}

// schedulingLoop continuously processes tasks from the queue
func (m *Master) schedulingLoop() {
	for task := range m.taskQueue {
		m.mu.Lock()

		// Try to schedule the task
		result := m.scheduler.Schedule(task, m.clusterState)

		if result.Success {
			log.Printf("[MASTER] ✓ Task %s scheduled on node %s (Score: %.2f)\n",
				task.Name, result.NodeID, result.Score)
			log.Printf("         %s\n", result.Reason)
		} else {
			log.Printf("[MASTER] ✗ Failed to schedule task %s: %s\n", task.Name, result.Reason)
		}

		m.mu.Unlock()

		// Simulate some processing delay
		time.Sleep(100 * time.Millisecond)
	}
}

// healthCheckLoop periodically checks if nodes are still healthy
func (m *Master) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.performHealthCheck()
		case <-m.done:
			return
		}
	}
}

// performHealthCheck verifies node health and updates state
func (m *Master) performHealthCheck() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, node := range m.clusterState.Nodes {
		// If node hasn't reported in 10 seconds, mark it as unhealthy
		if now.Sub(node.LastSeen) > 10*time.Second {
			node.Healthy = false
			log.Printf("[MASTER] ⚠ Node %s marked as unhealthy (no heartbeat)\n", node.Name)
		} else if !node.Healthy && now.Sub(node.LastSeen) < 5*time.Second {
			node.Healthy = true
			log.Printf("[MASTER] ✓ Node %s recovered\n", node.Name)
		}
	}
}

// GetClusterStatus returns the current state of the cluster
func (m *Master) GetClusterStatus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := scheduler.CalculateMetrics(m.clusterState)

	status := fmt.Sprintf(`
╔════════════════════════════════════════════╗
║          CLUSTER STATUS REPORT             ║
╚════════════════════════════════════════════╝

📊 Cluster Metrics:
  • Total Nodes: %d
  • Healthy Nodes: %d
  • Utilized Nodes: %d
  • Average CPU Usage: %.2f%%
  • Average Memory Usage: %.2f%%

💾 Resource Utilization:
  • Total CPU: %d cores (%d used)
  • Total Memory: %dMB (%dMB used)

📋 Nodes:
`,
		metrics.TotalNodes,
		metrics.HealthyNodes,
		metrics.UtilizedNodes,
		metrics.AverageCPUUsage,
		metrics.AverageMemUsage,
		metrics.TotalCPU/1000, metrics.UsedCPU/1000,
		metrics.TotalMemory, metrics.UsedMemory,
	)

	for _, node := range m.clusterState.Nodes {
		healthIcon := "✓"
		if !node.Healthy {
			healthIcon = "✗"
		}

		status += fmt.Sprintf(`
  [%s] %s
      CPU: %d/%d millicores | Memory: %dMB/%dMB | Tasks: %d
      Utilization: %.2f%%
`,
			healthIcon,
			node.Name,
			node.Total.CPU-node.Available.CPU, node.Total.CPU,
			node.Total.Memory-node.Available.Memory, node.Total.Memory,
			len(node.Tasks),
			node.GetUtilization(),
		)
	}

	return status
}

// ===== MAIN SIMULATION =====

func main() {
	master := NewMaster()
	master.Start()

	// Simulate registering worker nodes
	node1 := models.NewNode("node-1", "Worker-1", models.NewResource(4000, 8192, 50000))   // 4 cores, 8GB RAM
	node2 := models.NewNode("node-2", "Worker-2", models.NewResource(8000, 16384, 100000)) // 8 cores, 16GB RAM
	node3 := models.NewNode("node-3", "Worker-3", models.NewResource(2000, 4096, 25000))   // 2 cores, 4GB RAM

	master.RegisterNode(node1)
	master.RegisterNode(node2)
	master.RegisterNode(node3)

	// Simulate submitting various tasks
	tasks := []*models.Task{
		models.NewTask("task-1", "Data Processing Job", models.NewResource(500, 2048, 5000)),
		models.NewTask("task-2", "ML Training", models.NewResource(2000, 4096, 20000)),
		models.NewTask("task-3", "API Server", models.NewResource(1000, 1024, 2000)),
		models.NewTask("task-4", "Backup Service", models.NewResource(800, 512, 10000)),
		models.NewTask("task-5", "Large Analytics Job", models.NewResource(4000, 8192, 50000)),
		models.NewTask("task-6", "Cache Warmer", models.NewResource(500, 256, 1000)),
	}

	// Submit tasks
	for _, task := range tasks {
		go func(t *models.Task) {
			time.Sleep(time.Duration(len(t.ID)*100) * time.Millisecond)
			master.SubmitTask(t)
		}(task)
	}

	// Let tasks process
	time.Sleep(2 * time.Second)

	// Print cluster status
	fmt.Println(master.GetClusterStatus())

	// Simulate a node failure
	log.Println("\n[SIMULATION] Simulating node failure...")
	node2.LastSeen = time.Now().Add(-15 * time.Second)
	time.Sleep(6 * time.Second)

	fmt.Println(master.GetClusterStatus())

	// Cleanup
	master.Stop()
}