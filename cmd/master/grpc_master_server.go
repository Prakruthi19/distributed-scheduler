	package main

	import (
		"context"
		"fmt"
		"log"
		"net"
		"sync"
		"time"
		"sort"
		"google.golang.org/grpc"
		"mini-k8s-scheduler/internal/scheduler"
		"mini-k8s-scheduler/pkg/models"
		pb "mini-k8s-scheduler/pkg/proto"
	)

	// GRPCMasterServer implements the Master gRPC service
	type GRPCMasterServer struct {
		pb.UnimplementedMasterServer
		clusterState *models.ClusterState
		scheduler    *scheduler.BinPackingScheduler
		taskQueue    chan *models.Task
		mu           sync.RWMutex
		pendingTasks []*models.Task
		workerAddrs  map[string]string // node_id -> "host:port"
	}

	// NewGRPCMasterServer creates a new gRPC server
	func NewGRPCMasterServer() *GRPCMasterServer {
		return &GRPCMasterServer{
			clusterState: &models.ClusterState{
				Nodes: make(map[string]*models.Node),
			},
			scheduler:   scheduler.NewBinPackingScheduler("best-fit"),
			taskQueue:   make(chan *models.Task, 100),
			workerAddrs: make(map[string]string),
		}
	}

	// RegisterNode - Worker registers with Master
	func (s *GRPCMasterServer) RegisterNode(ctx context.Context, req *pb.NodeInfo) (*pb.RegisterResponse, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		log.Printf("[GRPC] Node registration request: %s\n", req.Name)

		// Create node
		resource := models.NewResource(req.Total.Cpu, req.Total.Memory, req.Total.Disk)
		node := models.NewNode(req.Id, req.Name, resource)
		node.Labels = req.Labels

		// Store node
		s.clusterState.Nodes[req.Id] = node
		s.workerAddrs[req.Id] = req.Address

		log.Printf("[GRPC] ✓ Node registered: %s (CPU: %d, Memory: %dMB) at %s\n", 
			req.Name, req.Total.Cpu, req.Total.Memory, req.Address)

		return &pb.RegisterResponse{
			Success: true,
			Message: fmt.Sprintf("Node %s registered successfully", req.Name),
			NodeId:  req.Id,
		}, nil
	}

	// Heartbeat - Worker sends periodic heartbeat
	func (s *GRPCMasterServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if node, exists := s.clusterState.Nodes[req.NodeId]; exists {
			// Update node status
			node.LastSeen = time.Now()
			node.Healthy = req.Healthy
			//node.Available = models.NewResource(req.Available.Cpu, req.Available.Memory, req.Available.Disk)

			log.Printf("[GRPC] Heartbeat from %s - CPU: %d/%d, Memory: %dMB/%dMB\n",
				node.Name,
				node.Total.CPU-node.Available.CPU, node.Total.CPU,
				node.Total.Memory-node.Available.Memory, node.Total.Memory)

			return &pb.HeartbeatResponse{
				Accepted: true,
				Message:  "Heartbeat received",
			}, nil
		}

		return &pb.HeartbeatResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Node %s not found", req.NodeId),
		}, nil
	}

	// GetTask - Worker requests a task
func (s *GRPCMasterServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
		s.mu.Lock()
    defer s.mu.Unlock()
		log.Printf("[GRPC] Task request from %s", req.NodeId)

    // 1. Find the worker's labels from the cluster state
    worker, exists := s.clusterState.Nodes[req.NodeId]
    if !exists {
        return &pb.Task{Id: "no-task", Status: "idle"}, nil
    }

    // 2. Search for the best task this specific worker can handle
    for i, task := range s.pendingTasks {
        // --- THE GATEKEEPER CHECK ---
        if matchesNodeSelector(task.NodeSelector, worker.Labels) {
            // We found a match! 
            taskToRun := task
            
            // Remove it from the queue (Pop from specific index)
            s.pendingTasks = append(s.pendingTasks[:i], s.pendingTasks[i+1:]...)

            log.Printf("[GRPC] 🚀 Sending %s (Priority %d) to %s (Match: %v)", 
                taskToRun.Name, taskToRun.Priority, req.NodeId, worker.Labels)

			return &pb.Task{
				Id:       taskToRun.ID,
				Name:     taskToRun.Name,
				Priority: taskToRun.Priority,
				Status:   "running",
			}, nil
		}
	}
	return &pb.Task{Id: "no-task", Status: "idle"}, nil
}
	// ReportTaskStatus - Worker reports task completion
func (s *GRPCMasterServer) ReportTaskStatus(ctx context.Context, req *pb.TaskStatusRequest) (*pb.TaskStatusResponse, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		log.Printf("[GRPC] Task status update: Task %s on Node %s - Status: %s\n",
			req.TaskId, req.NodeId, req.Status)

		if node, exists := s.clusterState.Nodes[req.NodeId]; exists {
			if req.Status == "completed" {
				// Remove task from node
				for i, tid := range node.Tasks {
					if tid == req.TaskId {
						node.Tasks = append(node.Tasks[:i], node.Tasks[i+1:]...)
						break
					}
				}
				log.Printf("[GRPC] ✓ Task %s completed on %s\n", req.TaskId, node.Name)
			}

			return &pb.TaskStatusResponse{
				Accepted: true,
				Message:  "Task status updated",
			}, nil
		}

		return &pb.TaskStatusResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Node %s not found", req.NodeId),
		}, nil
	}

	// SubmitTask - API to submit a task (for external clients)
	func (s *GRPCMasterServer) SubmitTask(task *models.Task) (*models.SchedulingResult, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		log.Printf("[GRPC] Task submitted: %s\n", task.Name)

		// Schedule the task
		result := s.scheduler.Schedule(task, s.clusterState)

		if !result.Success {
			log.Printf("No room for Task %s. Adding to Pending Queue.", task.Name)
			task.Status = "pending"
			s.pendingTasks = append(s.pendingTasks, task)
			s.sortPendingTasks()
		} else{
			log.Printf("✓ Task %s scheduled on node %s", task.Name, result.NodeID)
			// assignTaskToWorker(result.NodeID, task)
		}
		return result, nil
	}

	// StartServer starts the gRPC server
	func (s *GRPCMasterServer) StartServer(port string) error {
		lis, err := net.Listen("tcp", ":"+port)
		if err != nil {
			return fmt.Errorf("failed to listen: %v", err)
		}

		grpcServer := grpc.NewServer()
		pb.RegisterMasterServer(grpcServer, s)

		log.Printf("[GRPC] Master server listening on port %s\n", port)

		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("failed to serve: %v", err)
		}

		return nil
	}

	// Sort the quueue by priority (higher priority first)
	func (s *GRPCMasterServer) sortPendingTasks() {
		sort.Slice(s.pendingTasks, func(i, j int) bool {
			// We want the HIGHEST priority number at the front (Index 0)
			return s.pendingTasks[i].Priority > s.pendingTasks[j].Priority
		})
	}
	// GetClusterStatus returns cluster statistics
	func (s *GRPCMasterServer) GetClusterStatus() string {
		s.mu.RLock()
		defer s.mu.RUnlock()

		metrics := scheduler.CalculateMetrics(s.clusterState)

		status := fmt.Sprintf(`
	╔════════════════════════════════════════════╗
	║      GRPC CLUSTER STATUS REPORT            ║
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

	📋 Registered Nodes:
	`,
			metrics.TotalNodes,
			metrics.HealthyNodes,
			metrics.UtilizedNodes,
			metrics.AverageCPUUsage,
			metrics.AverageMemUsage,
			metrics.TotalCPU/1000, metrics.UsedCPU/1000,
			metrics.TotalMemory, metrics.UsedMemory,
		)

		for _, node := range s.clusterState.Nodes {
			healthIcon := "✓"
			if !node.Healthy {
				healthIcon = "✗"
			}

			addr := ""
			if a, exists := s.workerAddrs[node.ID]; exists {
				addr = a
			}

			status += fmt.Sprintf(`
	[%s] %s
		Address: %s
		CPU: %d/%d millicores | Memory: %dMB/%dMB | Tasks: %d
		Utilization: %.2f%%
	`,
				healthIcon,
				node.Name,
				addr,
				node.Total.CPU-node.Available.CPU, node.Total.CPU,
				node.Total.Memory-node.Available.Memory, node.Total.Memory,
				len(node.Tasks),
				node.GetUtilization(),
			)
		}
		status += "\n⏳ Pending Queue (Sorted by Priority):\n"
		if len(s.pendingTasks) == 0 {
			status += "  (No tasks waiting)\n"
		} else {
			for i, t := range s.pendingTasks {
				status += fmt.Sprintf("  %d. [%d] %s (Req: %dMB Memory)\n", 
					i+1, t.Priority, t.Name, t.Required.Memory)
			}
		}
		return status
	}

	// ===== MAIN ENTRY POINT =====

	func main() {
		server := NewGRPCMasterServer()

		// 1. Start gRPC server in background
		go func() {
			if err := server.StartServer("50051"); err != nil {
				log.Fatalf("Failed to start gRPC server: %v", err)
			}
		}()

		log.Println("[GRPC] Master gRPC server started on port 50051")
		
		
		// 2. Start the status monitor in the background so it doesn't block
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			for range ticker.C {
				fmt.Println(server.GetClusterStatus())
			}
		}()

		// 3. Wait for your worker to connect and register
		log.Println("[GRPC] Waiting for at least one worker to register...")
		for {
			server.mu.RLock()
			nodeCount := len(server.clusterState.Nodes)
			server.mu.RUnlock()
			
			if nodeCount > 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
		log.Println("[GRPC] Worker detected! Starting experiment...")

		fmt.Println("🧪 STARTING PRIORITY TEST...")

		// Task 1: Medium Priority - This should fit and run immediately

		t1 := models.NewTask("task-1", "Hog-Task", models.Resource{CPU: 500, Memory: 500}, 1, map[string]string{})

		// Task 2: Low Priority - This won't fit, goes to queue
		t2 := models.NewTask("task-2", "Low-Priority-Job", models.Resource{CPU: 500, Memory: 500}, 0, map[string]string{})

		// Task 3: High Priority (VIP) - This won't fit, should JUMP ahead of Task 2
		t3 := models.NewTask("task-3", "VIP-API-Server", models.Resource{CPU: 500, Memory: 500}, 2, map[string]string{"disk": "ssd"})

		server.SubmitTask(t1)
		server.SubmitTask(t2)
		server.SubmitTask(t3)
		// 5. Keep the main function alive so the server doesn't close
		select {} 
	}

func matchesNodeSelector(selector map[string]string, labels map[string]string) bool {
    if len(selector) == 0 {
        return true
    }
    for k, v := range selector {
        if labels[k] != v {
            return false
        }
    }
    return true
}