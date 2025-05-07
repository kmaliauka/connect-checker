package main

import (
	"bytes"
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

type ConnTask struct {
	connector string
	taskId    int
}

type TaskResp struct {
	Id    int    `json:"id"`
	State string `json:"state"`
}

type Responce struct {
	State string `json:"state"`
}

type StatusResp struct {
	Connector Responce   `json:"connector"`
	Tasks     []TaskResp `json:"tasks"`
}

func main() {
	kcenv := os.Getenv("KAFKA_CONNECT_URL")
	ConnectFmtUrl := flag.String("host", kcenv, "kafka connect ")
	CheckSleep := flag.Int("duration", 15, "check sleep duration")
	flag.Parse()
	fmt.Println(" URL " + *ConnectFmtUrl)
	fmt.Printf("DURATION BETWEEN CHECK %d mins", *CheckSleep)

	var ConUrlMain = *ConnectFmtUrl
	if !strings.HasPrefix(ConUrlMain, "http") {
		ConUrlMain = "http://" + ConUrlMain
	}
	if strings.HasSuffix(ConUrlMain, "/") {
		ConUrlMain = strings.Trim(ConUrlMain, "/")
	}

	for true {
		connectors, err := getConnectorsList(ConUrlMain)
		if err == nil {
			for _, connector := range connectors {
				go ConHandl(connector, ConUrlMain)
			}
		} else {
			log.Println(err)
		}
		time.Sleep(time.Duration(*CheckSleep) * time.Minute)
	}
}

func getConnectors(responseData []byte) []string {
	var connectors []string
	json.Unmarshal(responseData, &connectors)
	return connectors
}

func ConHandl(connector string, kafkaConnectHost string) {
	fmt.Println("Handling " + connector)
	var doRestart = RestartList(connector, kafkaConnectHost)
	for _, task := range doRestart {
		go restartConnTask(task, kafkaConnectHost)
	}

}

func restartConnTask(task ConnTask, ConUrlMain string) (*http.Response, error) {
	var url = ConUrlMain + "/connectors/" + task.connector + "/tasks/" + strconv.Itoa(task.taskId) + "/restart"
	fmt.Println(url)
	fmt.Printf("Going to restart "+task.connector+":%d", task.taskId)
	return http.Post(url, "application/json", bytes.NewReader([]byte{}))
}

func RestartList(connector string, ConUrlMain string) []ConnTask {
	var statusUrl = ConUrlMain + "/connectors/" + connector + "/status"
	resp, err := http.Get(statusUrl)
	if err != nil {
		log.Println(err)
		return []ConnTask{}
	} else {
		responseData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return []ConnTask{}
		} else {
			var status StatusResp
			json.Unmarshal(responseData, &status)
			var doRestart []ConnTask
			if status.Connector.State == "RUNNING" {
				for _, task := range status.Tasks {
					if task.State == "FAILED" {
						doRestart = append(doRestart, ConnTask{connector, task.Id})
					}
				}
			}
			return doRestart
		}

	}
}

func getConnectorsList(ConUrlMain string) ([]string, error) {
	url := ConUrlMain + "/connectors"
	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return nil, err
	} else {
		responseData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		return getConnectors(responseData), nil
	}
}
