package main

import (
	app "example/go-temporal-worker"
	"flag"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	hostPortPtr := flag.String("hostPort", client.DefaultHostPort, "Address of the Temporal server")
	flag.Parse()

	temporalClient, err := client.Dial(client.Options{HostPort: *hostPortPtr})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer temporalClient.Close()

	workerInstance := worker.New(temporalClient, "dbt-update-operations", worker.Options{})

	workerInstance.RegisterWorkflow(app.DbtParallelRefreshWorkflow)

	// TODO: register your Activity

	err = workerInstance.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
