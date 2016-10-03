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

	deleteOldSucceededTasks()
	go func() {
		for {
			select {
			case <-ticker.C:
				deleteOldSucceededTasks()
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

func deleteOldSucceededTasks() {
	DB.
		Unscoped().
		Where("state = ?", backends.SuccessState).
		Where("created_at < ?", time.Now().UTC().Add(-24*time.Hour)).
		Delete(&Task{})
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

func Last20Errors() []Metrics {
	tasks := make([]Task, 0)
	DB.Where("state = ?", backends.FailureState).
		Order("created_at DESC").
		Limit(20).
		Find(&tasks)

	m := []Metrics{}
	for _, t := range tasks {
		m = append(m, Metrics{
			"id":    t.UUID,
			"name":  t.Name,
			"error": t.Error,
		})
	}

	return m
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
