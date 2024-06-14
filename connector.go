package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type ConnectorJob struct {
	ConnectorName string
	JobID         int
}

type JobResponse struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

type Response struct {
	State string `json:"state"`
}

type StatusResponse struct {
	Connector Response      `json:"connector"`
	Jobs      []JobResponse `json:"jobs"`
}

func main() {
	kafkaConnectEnv := os.Getenv("KAFKA_CONNECT_URL")
	kafkaConnectURL := flag.String("host", kafkaConnectEnv, "Kafka Connect URL")
	checkSleepDuration := flag.Int("sleep", 5, "Check sleep duration")
	flag.Parse()

	fmt.Println("URL: " + *kafkaConnectURL)
	fmt.Printf("Duration between checks: %d mins\n", *checkSleepDuration)

	connectURL := *kafkaConnectURL
	if !strings.HasPrefix(connectURL, "http") {
		connectURL = "http://" + connectURL
	}
	if strings.HasSuffix(connectURL, "/") {
		connectURL = strings.TrimSuffix(connectURL, "/")
	}

	for {
		connectors, err := getConnectorsList(connectURL)
		if err == nil {
			for _, connector := range connectors {
				go handleConnector(connector, connectURL)
			}
		} else {
			log.Println(err)
		}
		time.Sleep(time.Duration(*checkSleepDuration) * time.Minute)
	}
}

// Get connector list
func getConnectorsList(connectURL string) ([]string, error) {
	url := connectURL + "/connectors"
	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return extractConnectors(responseData), nil
}

func extractConnectors(responseData []byte) []string {
	var connectors []string
	json.Unmarshal(responseData, &connectors)
	return connectors
}

func handleConnector(connector string, kafkaConnectURL string) {
	fmt.Println("Handling connector: " + connector)
	restartJobs := getJobsToRestart(connector, kafkaConnectURL)
	for _, job := range restartJobs {
		go restartConnectorJob(job, kafkaConnectURL)
	}
}

// Get connector statuses
func getJobsToRestart(connector string, connectURL string) []ConnectorJob {
	statusURL := connectURL + "/connectors/" + connector + "/status"
	resp, err := http.Get(statusURL)
	if err != nil {
		log.Println(err)
		return []ConnectorJob{}
	}

	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return []ConnectorJob{}
	}

	var status StatusResponse
	json.Unmarshal(responseData, &status)
	// Get tasks statuses
	var jobsToRestart []ConnectorJob
	if status.Connector.State == "RUNNING" {
		for _, job := range status.Jobs {
			if job.State == "FAILED" {
				jobsToRestart = append(jobsToRestart, ConnectorJob{connector, job.ID})
			}
		}
	}

	return jobsToRestart
}

// Restart running connector task if failed
func restartConnectorJob(job ConnectorJob, connectURL string) {
	url := connectURL + "/connectors/" + job.ConnectorName + "/jobs/" + strconv.Itoa(job.JobID) + "/restart"
	fmt.Println("Restarting job: " + url)
	fmt.Printf("Going to restart %s:%d\n", job.ConnectorName, job.JobID)
}
