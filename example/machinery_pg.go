package main

import (
	"flag"
	"fmt"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/mdouchement/machinery-pg"
)

var server *machinery.Server
var worker *machinery.Worker

var cnf = &config.Config{
	Broker:        "postgres://postgres:postgres@localhost:5432/machinery_testouille?sslmode=disable",
	ResultBackend: "postgres://postgres:postgres@localhost:5432/machinery_testouille?sslmode=disable",
	DefaultQueue:  "machinery_tasks",
	BindingKey:    "machinery_task",
}

func init() {
	brokerUrl := cnf.Broker
	backendUrl := cnf.ResultBackend
	var err error

	// Database migration
	err = machinerypg.MigrateBroker(cnf.Broker)
	check(err)

	// Server instanciation
	cnf.Broker = "eager://"
	cnf.ResultBackend = "eager://"
	server, err = machinery.NewServer(cnf)
	check(err)
	cnf.Broker = brokerUrl
	cnf.ResultBackend = backendUrl

	// Defines Postgres as broker and backend result
	broker := machinerypg.NewBroker(cnf)
	server.SetBroker(broker)
	backend := machinerypg.NewBackend(cnf)
	server.SetBackend(backend)

	// Register tasks
	tasks := map[string]interface{}{
		"add":      Add,
		"multiply": Multiply,
	}
	server.RegisterTasks(tasks)

	// Get a worker for the registered tasks
	worker = server.NewWorker("machinery_worker")
}

func main() {
	w := flag.Bool("w", false, "Launch worker")
	s := flag.Bool("s", false, "Send a task")
	sg := flag.Bool("sg", false, "Send a group of tasks")
	flag.Parse()

	if *w {
		fmt.Println("Starting worker...")
		err := worker.Launch()
		check(err)
	}

	task1 := &signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "int64",
				Value: 1,
			},
			signatures.TaskArg{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	task2 := &signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float32",
				Value: 2.5,
			},
			signatures.TaskArg{
				Type:  "float32",
				Value: 1.5,
			},
		},
	}

	if *s {
		fmt.Println("Sending a task...")

		asyncResult, err := server.SendTask(task1)
		check(err)

		result, err := asyncResult.Get() // Block until task is completed
		check(err)

		fmt.Println(result)
	}

	if *sg {
		fmt.Println("Sending a group tasks...")

		group := machinery.NewGroup(task1, task2)
		asyncResults, err := server.SendGroup(group)
		check(err)

		for _, asyncResult := range asyncResults {
			result, err := asyncResult.Get() // Block until task is completed
			check(err)

			fmt.Println(result)
		}
	}
}

// Add TASK ...
func Add(args ...int64) (int64, error) {
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	return sum, nil
}

func Multiply(args ...float32) (float32, error) {
	res := float32(1)
	for _, arg := range args {
		res *= arg
	}
	return res, nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
