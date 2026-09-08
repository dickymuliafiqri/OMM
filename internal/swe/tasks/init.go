package tasks

import "benchmark/internal/swe"

func init() {
	swe.RegisterTask(TaskT1OrderValidator)
	swe.RegisterTask(TaskT1InventoryCache)
	swe.RegisterTask(TaskT2RateLimiter)
	swe.RegisterTask(TaskT2WorkerPool)
	swe.RegisterTask(TaskT3DistributedCache)
	swe.RegisterTask(TaskT3ConnectionPool)
	swe.RegisterTask(TaskT4SagaOrchestrator)
	swe.RegisterTask(TaskT4MPMCRing)
}

// AllTasksList returns all 8 tasks for batch testing
func AllTasksList() []*swe.Task {
	return []*swe.Task{
		TaskT1OrderValidator,
		TaskT1InventoryCache,
		TaskT2RateLimiter,
		TaskT2WorkerPool,
		TaskT3DistributedCache,
		TaskT3ConnectionPool,
		TaskT4SagaOrchestrator,
		TaskT4MPMCRing,
	}
}
