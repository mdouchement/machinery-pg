package machinerypg

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Backend contains all stuff fot using Postgres as a Machinery backend result
type Backend struct {
}

// NewBackend creates new Postgres backend instance
func NewBackend(cnf *config.Config) backends.Backend {
	err := GormInit(cnf.ResultBackend)
	if err != nil {
		panic(fmt.Errorf("NewBackend: %s", err))
	}
	return &Backend{}
}

// InitGroup - saves UUIDs of all tasks in a group
func (pb *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	for _, taskUUID := range taskUUIDs {
		t := NewTaskWithID(taskUUID)
		t.GroupUUID = NGUUID(groupUUID)
		db := DB.FirstOrCreate(t)
		if db.Error != nil {
			return fmt.Errorf("InitGroup: %s", db.Error)
		}
	}
	return nil
}

// GroupCompleted - returns true if all tasks in a group finished
func (pb *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	tasks := make([]*Task, 0, groupTaskCount)
	if err := DB.Where("group_uuid = ?", GUUID(groupUUID)).Find(&tasks).Error; err != nil {
		return false, fmt.Errorf("GroupCompleted: %s", err)
	}

	countSuccessTasks := 0
	for _, task := range tasks {
		if !task.TaskState().IsCompleted() {
			return false, nil
		}
		countSuccessTasks++
	}
	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates - returns states of all tasks in the group
func (pb *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*backends.TaskState, error) {
	tasks := make([]*Task, 0, groupTaskCount)
	if db := DB.Where("group_uuid = ?", GUUID(groupUUID)).Find(&tasks); db.Error != nil {
		return nil, fmt.Errorf("GroupTaskStates: %s", db.Error)
	}

	taskStates := make([]*backends.TaskState, 0, groupTaskCount)
	for _, task := range tasks {
		taskStates = append(taskStates, task.TaskState())
	}

	return taskStates, nil
}

// SetStatePending - sets task state to PENDING
func (pb *Backend) SetStatePending(signature *signatures.TaskSignature) error {
	task := NewTask()
	if err := task.ApplySignature(signature); err != nil {
		return fmt.Errorf("SetStatePending: %s", err)
	}

	return DB.Model(task).Update("state", backends.PendingState).Error
}

// SetStateReceived - sets task state to RECEIVED
func (pb *Backend) SetStateReceived(signature *signatures.TaskSignature) error {
	task := NewTask()
	if err := task.ApplySignature(signature); err != nil {
		return fmt.Errorf("SetStateReceived: %s", err)
	}

	return DB.Model(task).Update("state", backends.ReceivedState).Error
}

// SetStateStarted - sets task state to STARTED
func (pb *Backend) SetStateStarted(signature *signatures.TaskSignature) error {
	task := NewTask()
	if err := task.ApplySignature(signature); err != nil {
		return fmt.Errorf("SetStateStarted: %s", err)
	}

	return DB.Model(task).Update("state", backends.StartedState).Error
}

// SetStateSuccess - sets task state to SUCCESS
func (pb *Backend) SetStateSuccess(signature *signatures.TaskSignature, result *backends.TaskResult) error {
	task := NewTask()
	if err := task.ApplySignature(signature); err != nil {
		return fmt.Errorf("SetStateSuccess: %s", err)
	}

	return DB.Model(task).Updates(map[string]interface{}{
		"result": task.MarshalResult(result),
		"state":  backends.SuccessState,
	}).Error
}

// SetStateFailure - sets task state to FAILURE
func (pb *Backend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	task := NewTask()
	if err := task.ApplySignature(signature); err != nil {
		return fmt.Errorf("SetStateFailure: %s", err)
	}

	return DB.Model(task).Updates(map[string]interface{}{
		"state": backends.FailureState,
		"error": err,
	}).Error
}

// GetState - returns the latest task state
func (pb *Backend) GetState(taskUUID string) (*backends.TaskState, error) {
	rateLimiter()

	task := NewTaskWithID(taskUUID)
	if err := DB.First(task).Error; err != nil {
		return nil, fmt.Errorf("GetState: %s", err)
	}
	return task.TaskState(), nil
}

// PurgeState - deletes stored task state
func (pb *Backend) PurgeState(taskUUID string) error {
	return DB.Delete(NewTaskWithID(taskUUID)).Error
}

// PurgeGroupMeta - deletes stored group meta data
func (pb *Backend) PurgeGroupMeta(groupUUID string) error {
	return DB.Where("group_uuid = ?", GUUID(groupUUID)).Delete(&Task{}).Error
}

// Reduce Machinery polling speed
func rateLimiter() {
	// Archaic rate limiter
	time.Sleep(4 * time.Second)
}
