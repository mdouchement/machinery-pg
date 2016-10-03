package machinerypg

import (
	"time"

	"github.com/RichardKnop/machinery/v1/backends"
)

// ------------------------- //
// Cleanup                   //
// ------------------------- //

var (
	// CleanupInterval in second.
	CleanupInterval = 10 * time.Minute
	quitCleanup     chan struct{}
)

// StartCleanupRoutine deletes the succeeded tasks older than 24 hours each CleanupInterval.
func StartCleanupRoutine() {
	ticker := time.NewTicker(CleanupInterval)
	quitCleanup = make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				DB.
					Unscoped().
					Where("state = ?", backends.SuccessState).
					Where("created_at < ?", time.Now().UTC().Add(-24*time.Hour)).
					Delete(&Task{})
			case <-quitCleanup:
				ticker.Stop()
				return
			}
		}
	}()
}

// StopCleanup stops cleanup routine.
func StopCleanup() {
	close(quitCleanup)
}

// ------------------------- //
// Metrics                   //
// ------------------------- //

// PendingTasks returns all metrics of pending tasks.
func PendingTasks() Metrics {
	return countTasks(backends.PendingState)
}

// RunningTasks returns all metrics of running tasks.
func RunningTasks() Metrics {
	return countTasks(backends.StartedState)
}

// CompletedTasks returns all metrics of completed tasks.
func CompletedTasks() Metrics {
	return countTasks(backends.SuccessState)
}

// FailedTasks returns all metrics of failed tasks.
func FailedTasks() Metrics {
	return countTasks(backends.FailureState)
}

func countTasks(state string) Metrics {
	tasks := make([]Task, 0)
	DB.Where("state = ?", state).
		Find(&tasks)

	m := Metrics{}
	for _, t := range tasks {
		m[t.Name] = m.Int(t.Name) + 1
	}

	return m
}
