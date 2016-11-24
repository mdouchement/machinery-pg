package machinerypg

import (
	"fmt"
	"sync"
	"time"

	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/lib/pq"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
)

// Broker contains all stuff fot using Postgres as a Machinery broker
type Broker struct {
	registeredTaskNames []string
	retry               bool
	retryFunc           func()
	stopChan            chan int
	stopReceivingChan   chan int
	errorsChan          chan error
	maxParallelTasks    int
	limiter             chan struct{}
	wg                  sync.WaitGroup
	mu                  sync.Mutex
}

// NewBroker creates new Postgres broker instance
func NewBroker(cnf *config.Config) brokers.Broker {
	err := GormInit(cnf.Broker)
	if err != nil {
		panic(fmt.Errorf("NewBroker: %s", err))
	}
	return &Broker{
		maxParallelTasks: 6,
		retry:            true,
	}
}

func (pb *Broker) MaxParallelTasks() int {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	return pb.maxParallelTasks
}

func (pb *Broker) SetMaxParallelTasks(n int) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.maxParallelTasks = n
	pb.limiter = make(chan struct{}, pb.maxParallelTasks)
}

// SetRegisteredTaskNames sets registered task names
func (pb *Broker) SetRegisteredTaskNames(names []string) {
	pb.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (pb *Broker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range pb.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// StartConsuming enters a loop and waits for incoming messages
func (pb *Broker) StartConsuming(consumerTag string, taskProcessor brokers.TaskProcessor) (bool, error) {
	if pb.retryFunc == nil {
		pb.retryFunc = utils.RetryClosure()
	}

	pb.retryFunc = utils.RetryClosure()
	pb.stopChan = make(chan int)
	pb.stopReceivingChan = make(chan int)
	pb.errorsChan = make(chan error)
	deliveries := make(chan *Task)

	if err := DB.DB().Ping(); err != nil {
		// Machinery polls StartConsuming so retryFunc is called and blocks the polling.
		pb.retryFunc()
		return pb.retry, err // retry true
	}

	pb.wg.Add(1)
	go func() {
		defer pb.wg.Done()
		ticker := time.NewTicker(1 * time.Second) // Use notfication instead polling?

		fmt.Println("[*] Waiting for messages. To exit press CTRL+C")
		for {
			select {
			// A way to stop this goroutine from redisBroker.StopConsuming
			case <-pb.stopReceivingChan:
				return
			case <-ticker.C:
				task := NewTask()

				// Start transaction to ensure there is no race condition
				tx := DB.Begin()

				err := tx.Set("gorm:query_option", "FOR UPDATE").
					Where("consumed = ?", false).
					Where("raw_task != '{}'").
					Where("name in (?)", pb.registeredTaskNames).
					Order("created_at").
					First(task).Error
				if tx.Error != nil {
					pb.errorsChan <- fmt.Errorf("StartConsuming: %s", err)
				}

				if task.UUID != "" {
					err = tx.Model(task).Update("consumed", true).Error
					if tx.Error != nil {
						pb.errorsChan <- fmt.Errorf("StartConsuming: %s", err)
					}
				}

				if err := DB.DB().Ping(); err != nil {
					// Start retry if connection refused
					// NOTE: tx.Commit() panics when connection refused error occurred
					pb.errorsChan <- fmt.Errorf("StartConsuming: %s", err)
					break
				} else {
					// End of the transaction
					tx.Commit()
				}

				if tx.Error != nil {
					pb.errorsChan <- fmt.Errorf("StartConsuming: %s", tx.Error)
				}

				if task.UUID != "" {
					deliveries <- task
				}
			}
		}
	}()

	if err := pb.consume(deliveries, taskProcessor); err != nil {
		return pb.retry, err // retry true
	}

	return pb.retry, nil
}

// StopConsuming quits the loop
func (pb *Broker) StopConsuming() {
	// Do not retry from now on
	pb.retry = false
	// Stop the receiving goroutine
	pb.stopReceiving()
	// Notifying the stop channel stops consuming of messages
	pb.stopChan <- 1
}

// Publish places a new message on the default queue
func (pb *Broker) Publish(task *signatures.TaskSignature) error {
	t := NewTask()
	if err := t.ApplySignature(task); err != nil {
		return fmt.Errorf("Publish: %s", err)
	}

	tx := DB.Begin()

	count := -1
	tx.Model(&Task{}).Where("UUID = ?", UUID(t.UUID)).Count(&count)
	if count == 0 {
		t.State = backends.PendingState
		tx.Create(t)
	} else {
		tx.Model(t).Updates(map[string]interface{}{
			"Name":      t.Name,
			"GroupUUID": t.GroupUUID,
			"RawTask":   t.RawTask,
		})
	}

	tx.Commit()

	if tx.Error != nil {
		return fmt.Errorf("Publish: %s", tx.Error)
	}

	return nil
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (pb *Broker) GetPendingTasks(queue string) ([]*signatures.TaskSignature, error) {
	tasks := []*Task{}

	db := DB.Where("consumed = ?", false).
		Where("name in (?)", pb.registeredTaskNames).
		Find(tasks)

	if db.Error != nil {
		return nil, fmt.Errorf("GetPendingTasks: %s", db.Error)
	}

	sigs := make([]*signatures.TaskSignature, 0, len(tasks))
	for _, task := range tasks {
		sig, err := task.Signature()
		if err != nil {
			return nil, fmt.Errorf("GetPendingTasks: %s", err)
		}
		sigs = append(sigs, sig)
	}

	return sigs, nil
}

// Consume a single message
func (pb *Broker) consumeOne(task *Task, taskProcessor brokers.TaskProcessor) {
	logg.Printf("Received new message: %s - %s", task.UUID, task.Name)

	sig, err := task.Signature()
	if err != nil {
		pb.errorsChan <- err
	}

	if err := taskProcessor.Process(sig); err != nil {
		pb.errorsChan <- err
	}
}

// Consumes messages...
func (pb *Broker) consume(deliveries <-chan *Task, taskProcessor brokers.TaskProcessor) error {
	if pb.limiter == nil {
		pb.limiter = make(chan struct{}, pb.maxParallelTasks)
	}

	for {
		select {
		case err := <-pb.errorsChan:
			return err
		case d := <-deliveries:
			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently according to the limiter
			pb.limiter <- struct{}{}
			go func() {
				defer func() { <-pb.limiter }()
				pb.consumeOne(d, taskProcessor)
			}()
		case <-pb.stopChan:
			return nil
		}
	}
}

// Stops the receiving goroutine
func (pb *Broker) stopReceiving() {
	pb.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	pb.wg.Wait()
	// Draining limiter channel
	close(pb.limiter)
}
