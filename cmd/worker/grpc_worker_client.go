package main

import (
	"context"
	"fmt"
	"flag"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"mini-k8s-scheduler/pkg/models"
	pb "mini-k8s-scheduler/pkg/proto"
)

// GRPCWorkerClient connects to Master
type GRPCWorkerClient struct {
	node       *models.Node
	masterAddr string
	conn       *grpc.ClientConn
	client     pb.MasterClient
	running    bool
	Labels    map[string]string
}

// NewGRPCWorkerClient creates a new worker client
func NewGRPCWorkerClient(nodeID, nodeName, masterAddr string, totalResources models.Resource, labels map[string]string) *GRPCWorkerClient {
	return &GRPCWorkerClient{
		node:       models.NewNode(nodeID, nodeName, totalResources),
		masterAddr: masterAddr,
		running:    false,
		Labels:    labels,
	}
}

// Connect establishes connection to Master
func (w *GRPCWorkerClient) Connect() error {
	log.Printf("[WORKER gRPC] Connecting to Master at %s\n", w.masterAddr)

	conn, err := grpc.Dial(w.masterAddr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to Master: %v", err)
	}

	w.conn = conn
	w.client = pb.NewMasterClient(conn)

	log.Printf("[WORKER gRPC] ✓ Connected to Master\n")
	return nil
}

// Register registers the worker with Master
func (w *GRPCWorkerClient) Register(workerListenAddr string) error {
	log.Printf("[WORKER gRPC] Registering with Master\n")

	req := &pb.NodeInfo{
		Id:      w.node.ID,
		Name:    w.node.Name,
		Total:   &pb.Resource{Cpu: w.node.Total.CPU, Memory: w.node.Total.Memory, Disk: w.node.Total.Disk},
		Address: workerListenAddr,
		Labels:  w.Labels,
	}

	resp, err := w.client.RegisterNode(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to register: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	log.Printf("[WORKER gRPC] ✓ Registered: %s\n", resp.Message)
	return nil
}

// Start begins the worker's heartbeat and task handling loops
func (w *GRPCWorkerClient) Start() {
	w.running = true
	log.Printf("[WORKER gRPC] Starting heartbeat and task loops\n")
	go w.heartbeatLoop()
	go w.taskRequestLoop()
}

// Stop halts the worker
func (w *GRPCWorkerClient) Stop() {
	w.running = false
	if w.conn != nil {
		w.conn.Close()
	}
	log.Printf("[WORKER gRPC] Stopped\n")
}

// heartbeatLoop sends periodic heartbeats to Master
func (w *GRPCWorkerClient) heartbeatLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for w.running {
		select {
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

// sendHeartbeat reports worker status to Master
func (w *GRPCWorkerClient) sendHeartbeat() {
	available := w.GetSystemResources()

	req := &pb.HeartbeatRequest{
		NodeId:       w.node.ID,
		Available:    &pb.Resource{Cpu: available.CPU, Memory: available.Memory, Disk: available.Disk},
		RunningTasks: w.node.Tasks,
		Healthy:      w.node.Healthy,
	}

	resp, err := w.client.Heartbeat(context.Background(), req)
	if err != nil {
		log.Printf("[WORKER gRPC] ✗ Heartbeat failed: %v\n", err)
		return
	}

	if resp.Accepted {
		log.Printf("[WORKER gRPC] Heartbeat sent - CPU: %d/%d, Memory: %dMB/%dMB, Tasks: %d\n",
			w.node.Total.CPU-w.node.Available.CPU, w.node.Total.CPU,
			w.node.Total.Memory-w.node.Available.Memory, w.node.Total.Memory,
			len(w.node.Tasks))
	}
}

// taskRequestLoop periodically asks Master for tasks
func (w *GRPCWorkerClient) taskRequestLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for w.running {
		select {
		case <-ticker.C:
			w.requestTask()
		}
	}
}

// requestTask asks Master if there are tasks to execute
func (w *GRPCWorkerClient) requestTask() {
	req := &pb.GetTaskRequest{
		NodeId: w.node.ID,
	}

	resp, err := w.client.GetTask(context.Background(), req)
	if err != nil {
		log.Printf("[WORKER gRPC] Failed to get task: %v\n", err)
		return
	}

	if resp.Status != "idle" && resp.Id != "no-task" {
		log.Printf("[WORKER gRPC] ✓ Received task: %s\n", resp.Name)
		w.executeTask(resp)
	}
}

// executeTask simulates task execution
func (w *GRPCWorkerClient) executeTask(task *pb.Task) {
	log.Printf("[WORKER gRPC] Executing task: %s\n", task.Name)

	w.node.Tasks = append(w.node.Tasks, task.Id)

	// Simulate execution
	time.Sleep(2 * time.Second)

	// Report completion
	req := &pb.TaskStatusRequest{
		NodeId: w.node.ID,
		TaskId: task.Id,
		Status: "completed",
		Message: fmt.Sprintf("Task %s completed successfully", task.Name),
	}

	resp, err := w.client.ReportTaskStatus(context.Background(), req)
	if err != nil {
		log.Printf("[WORKER gRPC] Failed to report task status: %v\n", err)
		return
	}

	if resp.Accepted {
		log.Printf("[WORKER gRPC] ✓ Task completion reported: %s\n", task.Name)
		// Remove from tasks
		for i, tid := range w.node.Tasks {
			if tid == task.Id {
				w.node.Tasks = append(w.node.Tasks[:i], w.node.Tasks[i+1:]...)
				break
			}
		}
	}
}

// GetSystemResources simulates reading actual system resources
func (w *GRPCWorkerClient) GetSystemResources() models.Resource {
	// Simulate some resource fluctuation
	cpuVariance := rand.Intn(500) - 250
	memVariance := rand.Intn(512) - 256

	available := models.Resource{
		CPU:    w.node.Available.CPU + int64(cpuVariance),
		Memory: w.node.Available.Memory + int64(memVariance),
		Disk:   w.node.Available.Disk,
	}

	// Ensure positive values
	if available.CPU < 0 {
		available.CPU = 0
	}
	if available.Memory < 0 {
		available.Memory = 0
	}

	return available
}

// GetStatus returns worker's current status
func (w *GRPCWorkerClient) GetStatus() string {
	return fmt.Sprintf(`
╔════════════════════════════════════════════╗
║      WORKER gRPC STATUS - %s             ║
╚════════════════════════════════════════════╝

📍 Worker Information:
  • ID: %s
  • Name: %s
  • Master: %s
  • Connected: %v
  • Healthy: %v

💾 Resource Status:
  • CPU: %d/%d millicores (%.2f%% used)
  • Memory: %dMB/%dMB (%.2f%% used)
  • Disk: %dMB/%dMB

📋 Running Tasks: %d
`,
		w.node.Name,
		w.node.ID,
		w.node.Name,
		w.masterAddr,
		w.conn != nil,
		w.node.Healthy,
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

// ===== MAIN ENTRY POINT =====

func main() {
	rand.Seed(time.Now().UnixNano())
	workerID := flag.String("id", "worker-1", "Unique ID for the worker")
    diskType := flag.String("disk", "ssd", "Type of disk (hdd or ssd)")
    
	flag.Parse()

	labels := map[string]string{
        "disk": *diskType,
    }
	// Create worker
	worker := NewGRPCWorkerClient(
		*workerID,
        *workerID,
		"localhost:50051", // Master address
		models.NewResource(4000, 1024, 50000), // 4 CPUs, 8GB RAM, 50GB Disk
		labels, 
	)

	// Connect to Master
	if err := worker.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer worker.Stop()

	// Register with Master
	if err := worker.Register("localhost:50052"); err != nil {
		log.Fatalf("Failed to register: %v", err)
	}

	// Start heartbeat and task loops
	worker.Start()

	log.Println("[WORKER gRPC] Worker started successfully")
	log.Println("[WORKER gRPC] Waiting for tasks from Master...")

	// Print status periodically
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println(worker.GetStatus())
	}
}

