package machinerypg

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// UUID removes the prefix from the given taskUUID
func UUID(taskUUID string) string {
	return strings.Replace(taskUUID, "task_", "", -1)
}

// NGUUID removes the group prefix from the given groupUUID or return nil if groupUUID is empty
func NGUUID(groupUUID string) *string {
	if groupUUID == "" {
		return nil
	}
	uuid := GUUID(groupUUID)
	return &uuid
}

// GUUID removes the group prefix from the given groupUUID
func GUUID(groupUUID string) string {
	return strings.Replace(groupUUID, "group_", "", -1)
}

// Task model represents the Machinery's signatures.TaskSignature in Postgres
type Task struct {
	// gorm.Model without ID field
	CreatedAt *time.Time
	UpdatedAt *time.Time
	DeletedAt *time.Time

	// signatures.TaskSignature
	UUID      string `gorm:"primary_key;type:uuid"`
	Name      string
	GroupUUID *string `gorm:"index;type:uuid"` // *string can be nil/NULL

	// Broker
	Consumed bool
	RawTask  []byte `gorm:"type:jsonb"` // try *json.RawMessage -> https://github.com/lib/pq/issues/437

	// Backend
	State  string `gorm:"index;not null"` // backend - ENUM type is not supportted by libpq
	Result []byte `gorm:"type:jsonb"`
	Error  string
}

// NewTask instanciates a new Task
func NewTask() *Task {
	return &Task{
		RawTask: []byte("{}"),
		Result:  []byte("{}"),
	}
}

// NewTask instanciates a new Task with the givent taskUUID
func NewTaskWithID(taskUUID string) *Task {
	return &Task{
		RawTask: []byte("{}"),
		Result:  []byte("{}"),
		UUID:    UUID(taskUUID),
	}
}

// ApplySignature adds the task to the current Task model
func (t *Task) ApplySignature(task *signatures.TaskSignature) error {
	t.UUID = UUID(task.UUID)
	t.Name = task.Name
	t.GroupUUID = NGUUID(task.GroupUUID)

	raw, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("ApplySignature: %s", err)
	}
	t.RawTask = raw

	return nil
}

// Signature returns the signature object serialzed in this Task Model
// It returns an error if unmarshalization fails
func (t *Task) Signature() (*signatures.TaskSignature, error) {
	task := &signatures.TaskSignature{}
	if err := json.Unmarshal(t.RawTask, task); err != nil {
		return nil, fmt.Errorf("Signature: %s", err)
	}
	return task, nil
}

// TaskState returns a Machinery TaskState according to the State of the Task
func (t *Task) TaskState() *backends.TaskState {
	taskState := &backends.TaskState{
		TaskUUID: "task_" + t.UUID,
		State:    t.State,
	}

	if taskState.State == backends.SuccessState {
		taskState.Result = t.UnmarshalResult()
	} else if taskState.State == backends.FailureState {
		taskState.Error = t.Error
	}
	return taskState
}

// MarshalResult serialzes the result in JSON
func (t *Task) MarshalResult(result *backends.TaskResult) []byte {
	r, err := json.Marshal(result)
	if err != nil {
		panic(fmt.Errorf("MarshalResult: %s", err))
	}
	return r
}

// UnmarshalResult unserialzes the result from JSON
func (t *Task) UnmarshalResult() *backends.TaskResult {
	result := &backends.TaskResult{}
	if err := json.Unmarshal(t.Result, result); err != nil {
		panic(fmt.Errorf("UnmarshalResult: %s", err))
	}
	return result
}
