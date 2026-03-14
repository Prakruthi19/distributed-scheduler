package scheduler

import (
	"fmt"
	"math"
	"sort"

	"mini-k8s-scheduler/pkg/models"
)

// Algorithm defines the scheduler algorithm interface
type Algorithm interface {
	Schedule(task *models.Task, clusterState *models.ClusterState) *models.SchedulingResult
}

// BinPackingScheduler implements First Fit Decreasing (FFD) bin packing algorithm
type BinPackingScheduler struct {
	algorithm string // "first-fit", "best-fit", "worst-fit"
}

// NewBinPackingScheduler creates a new scheduler with specified algorithm
func NewBinPackingScheduler(algorithm string) *BinPackingScheduler {
	return &BinPackingScheduler{
		algorithm: algorithm,
	}
}

// Schedule places a task on the best-fit node using bin packing
func (bps *BinPackingScheduler) Schedule(task *models.Task, clusterState *models.ClusterState) *models.SchedulingResult {
	result := &models.SchedulingResult{
		TaskID:  task.ID,
		Success: false,
		Reason:  "No suitable node found",
		Score:   0,
	}

	// Get list of candidate nodes (nodes that can fit the task)
	candidates := bps.getCandidateNodes(task, clusterState)
	
	if len(candidates) == 0 {
		result.Reason = "Insufficient resources on all nodes"
		return result
	}

	// Select best node based on algorithm
	var bestNode *models.Node
	var bestScore float64

	switch bps.algorithm {
	case "first-fit":
		bestNode, bestScore = bps.firstFit(candidates)
	case "best-fit":
		bestNode, bestScore = bps.bestFit(candidates)
	case "worst-fit":
		bestNode, bestScore = bps.worstFit(candidates)
	default:
		// Default to best-fit (most popular strategy)
		bestNode, bestScore = bps.bestFit(candidates)
	}

	if bestNode == nil {
		return result
	}

	// Allocate task to best node
	bestNode.AllocateResources(task)
	task.AssignedNode = bestNode.ID
	task.Status = "running"

	result.NodeID = bestNode.ID
	result.Success = true
	result.Reason = fmt.Sprintf("Task scheduled on node %s", bestNode.Name)
	result.Score = bestScore

	return result
}

// getCandidateNodes returns nodes that have enough resources for the task
func (bps *BinPackingScheduler) getCandidateNodes(task *models.Task, clusterState *models.ClusterState) []*models.Node {
	var candidates []*models.Node

	for _, node := range clusterState.Nodes {
		if !matchesNodeSelector(task.NodeSelector, node.Labels) {
            continue 
        }
		if node.CanFit(task) {
			candidates = append(candidates, node)
		}
	}

	return candidates
}

// firstFit returns the first node that can fit the task (simple, fast)
func (bps *BinPackingScheduler) firstFit(candidates []*models.Node) (*models.Node, float64) {
	if len(candidates) == 0 {
		return nil, 0
	}
	
	score := bps.calculateScore(candidates[0])
	return candidates[0], score
}

// bestFit returns the node with the least remaining resources (minimizes waste)
func (bps *BinPackingScheduler) bestFit(candidates []*models.Node) (*models.Node, float64) {
	if len(candidates) == 0 {
		return nil, 0
	}

	bestNode := candidates[0]
	bestScore := math.MaxFloat64

	for _, node := range candidates {
		score := bps.calculateScore(node)
		
		// Lower score = better fit (less wasted resources)
		if score < bestScore {
			bestScore = score
			bestNode = node
		}
	}

	return bestNode, bestScore
}

// worstFit returns the node with the most remaining resources (spreads load)
func (bps *BinPackingScheduler) worstFit(candidates []*models.Node) (*models.Node, float64) {
	if len(candidates) == 0 {
		return nil, 0
	}

	worstNode := candidates[0]
	worstScore := float64(-1)

	for _, node := range candidates {
		score := bps.calculateScore(node)
		
		// Higher score = more free resources
		if score > worstScore {
			worstScore = score
			worstNode = node
		}
	}

	return worstNode, worstScore
}

// calculateScore computes a score for a node (lower = better fit for best-fit)
// This considers remaining resources and fragmentation
func (bps *BinPackingScheduler) calculateScore(node *models.Node) float64 {
	if node == nil {
		return math.MaxFloat64
	}

	// Normalize available resources (0 to 100)
	cpuPercent := float64(node.Available.CPU) / float64(node.Total.CPU) * 100
	memPercent := float64(node.Available.Memory) / float64(node.Total.Memory) * 100
	diskPercent := float64(node.Available.Disk) / float64(node.Total.Disk) * 100

	// Average remaining capacity
	avgRemaining := (cpuPercent + memPercent + diskPercent) / 3

	// Penalize fragmented nodes (lower is better)
	// Nodes with many small tasks are harder to place large tasks
	fragmentationPenalty := float64(len(node.Tasks)) * 0.5

	return avgRemaining + fragmentationPenalty
}

// ScheduleMultiple schedules multiple tasks and returns results for each
func (bps *BinPackingScheduler) ScheduleMultiple(tasks []*models.Task, clusterState *models.ClusterState) []*models.SchedulingResult {
	// Sort tasks by required resources (largest first - FFD strategy)
	sort.Slice(tasks, func(i, j int) bool {
		resourceI := tasks[i].Required.CPU + tasks[i].Required.Memory + tasks[i].Required.Disk
		resourceJ := tasks[j].Required.CPU + tasks[j].Required.Memory + tasks[j].Required.Disk
		return resourceI > resourceJ
	})

	var results []*models.SchedulingResult
	for _, task := range tasks {
		result := bps.Schedule(task, clusterState)
		results = append(results, result)
	}

	return results
}

// GetClusterMetrics returns statistics about the cluster
type ClusterMetrics struct {
	TotalNodes      int
	HealthyNodes    int
	UtilizedNodes   int
	TotalCPU        int64
	TotalMemory     int64
	UsedCPU         int64
	UsedMemory      int64
	AverageCPUUsage float64
	AverageMemUsage float64
}

// CalculateMetrics computes cluster statistics
func CalculateMetrics(clusterState *models.ClusterState) ClusterMetrics {
	metrics := ClusterMetrics{}

	for _, node := range clusterState.Nodes {
		metrics.TotalNodes++
		metrics.TotalCPU += node.Total.CPU
		metrics.TotalMemory += node.Total.Memory

		if node.Healthy {
			metrics.HealthyNodes++
		}

		if len(node.Tasks) > 0 {
			metrics.UtilizedNodes++
		}

		metrics.UsedCPU += node.Total.CPU - node.Available.CPU
		metrics.UsedMemory += node.Total.Memory - node.Available.Memory
	}

	if metrics.TotalCPU > 0 {
		metrics.AverageCPUUsage = float64(metrics.UsedCPU) / float64(metrics.TotalCPU) * 100
	}

	if metrics.TotalMemory > 0 {
		metrics.AverageMemUsage = float64(metrics.UsedMemory) / float64(metrics.TotalMemory) * 100
	}

	return metrics
}

func matchesNodeSelector(selector map[string]string, labels map[string]string) bool {
    // If no selector is defined, the task is happy anywhere
    if len(selector) == 0 {
        return true
    }

    // Check every key-value pair in the task's selector
    for key, requiredVal := range selector {
        nodeVal, exists := labels[key]
        if !exists || nodeVal != requiredVal {
            return false
        }
    }
    return true
}