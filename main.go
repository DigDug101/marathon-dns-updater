package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"github.com/r3labs/sse"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type MarathonAPI struct {
	Client *http.Client
	Host   string
	Path   string
}

func (api *MarathonAPI) urlForPath(path []string) string {
	fullPath := append([]string{api.Host, api.Path}, path...)
	return strings.Join(fullPath, "/")
}

func (api *MarathonAPI) rawRequest(method string, path []string, body interface{}) (*http.Response, error) {
	url := api.urlForPath(path)
	bodyJson, err := json.Marshal(body)

	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(bodyJson)
	req, err := http.NewRequest(method, url, buf)

	if err != nil {
		return nil, err
	}

	return api.Client.Do(req)
}

func (api *MarathonAPI) getApp(appId string) (*AppResponse, error) {
	resp, err := api.rawRequest("GET", []string{"apps", appId}, nil)

	if err != nil {
		return nil, err
	}

	if (resp.StatusCode / 100) != 2 {
		return nil, errors.New(fmt.Sprintf("Received non-2XX status in response: %v", *resp))
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	var app AppResponse
	if err = json.Unmarshal(body, &app); err != nil {
		return nil, err
	}

	return &app, nil
}

func (api *MarathonAPI) getEvents(ch chan *sse.Event) error {
	url := api.urlForPath([]string{"events"})
	client := sse.NewClient(url)

	return client.SubscribeChan("", ch)
}

func main() {
	host := flag.String("marathon-host", "http://marathon.mesos:8080", "HTTP endpoint of Marathon service")
	appId := flag.String("app-id", "marathon-lb", "Marathon app id of marathon-lb service")
	hostedZoneId := flag.String("hosted-zone-id", "", "Route53 Hosted Zone")
	recordSetName := flag.String("record-set", "marathon-lb.example.com", "Record set to update")
	interval := flag.Int64("interval", 360, "Update interval")

	flag.Parse()

	if *hostedZoneId == "" {
		log.Println("Hosted zone id is required")
		flag.Usage()
		os.Exit(1)
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	api := &MarathonAPI{
		Client: client,
		Host:   *host,
		Path:   "v2",
	}

	for {
		// Fetch running marathon-lb tasks
		app, err := api.getApp(*appId)
		if err != nil {
			log.Printf("%v", err)
			log.Fatalf("Unable to fetch appId: %s from host: %s", *appId, *host)
		}
		log.Printf("Successfully fetched app.")

		taskIps := make(map[string]string)
		for _, task := range app.App.Tasks {
			log.Printf("Processing task: %v", task.ID)
			if task.State != TaskRunning {
				continue
			}

			for _, ip := range task.IPAddresses {
				if ip.Protocol != "IPv4" {
					continue
				}
				taskIps[ip.IPAddress] = ip.IPAddress
			}
		}
		if len(taskIps) == 0 {
			log.Fatal("No running tasks found.")
		}

		// Update Route53
		sess := session.Must(session.NewSession())
		r53 := route53.New(sess)
		var changes []*route53.Change

		// Delete out of date records
		recordSets, err := r53.ListResourceRecordSets(&route53.ListResourceRecordSetsInput{
			HostedZoneId:    hostedZoneId,
			StartRecordName: recordSetName,
			StartRecordType: aws.String(route53.RRTypeA),
		})
		for _, recordSet := range recordSets.ResourceRecordSets {
			if len(recordSet.ResourceRecords) > 0 {
				record := recordSet.ResourceRecords[0]
				if taskIps[*record.Value] == "" {
					log.Printf("Marking record set %s for deletion", recordSet.String())
					recordDelete := &route53.Change{
						Action:            aws.String(route53.ChangeActionDelete),
						ResourceRecordSet: recordSet,
					}

					changes = append(changes, recordDelete)
				}
			}
		}

		// Ensure records for running tasks
		for _, ip := range taskIps {
			record := &route53.ResourceRecord{
				Value: aws.String(ip),
			}
			recordSet := &route53.ResourceRecordSet{
				Name:            recordSetName,
				Type:            aws.String(route53.RRTypeA),
				TTL:             aws.Int64(60),
				Weight:          aws.Int64(10),
				SetIdentifier:   record.Value,
				ResourceRecords: []*route53.ResourceRecord{record},
			}
			recordUpsert := &route53.Change{
				Action:            aws.String(route53.ChangeActionUpsert),
				ResourceRecordSet: recordSet,
			}
			log.Printf("Creating record set %s", recordSet)
			changes = append(changes, recordUpsert)
		}

		changeInput := &route53.ChangeResourceRecordSetsInput{
			ChangeBatch: &route53.ChangeBatch{
				Changes: changes,
				Comment: aws.String(fmt.Sprintf("Updated records for %s", *recordSetName)),
			},
			HostedZoneId: hostedZoneId,
		}

		// Start transaction
		result, err := r53.ChangeResourceRecordSets(changeInput)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case route53.ErrCodeNoSuchHostedZone:
					log.Println(route53.ErrCodeNoSuchHostedZone, aerr.Error())
				case route53.ErrCodeNoSuchHealthCheck:
					log.Println(route53.ErrCodeNoSuchHealthCheck, aerr.Error())
				case route53.ErrCodeInvalidChangeBatch:
					log.Println(route53.ErrCodeInvalidChangeBatch, aerr.Error())
				case route53.ErrCodeInvalidInput:
					log.Println(route53.ErrCodeInvalidInput, aerr.Error())
				case route53.ErrCodePriorRequestNotComplete:
					log.Println(route53.ErrCodePriorRequestNotComplete, aerr.Error())
				default:
					log.Println(aerr.Error())
				}
			} else {
				log.Println(err.Error())
			}

			continue
		}

		// Wait for transaction to complete
		waitInput := &route53.GetChangeInput{
			Id: result.ChangeInfo.Id,
		}
		err = r53.WaitUntilResourceRecordSetsChanged(waitInput)

		if err != nil {
			log.Printf("Error updating record set: %v", err)
		} else {
			log.Printf("Updated record set for %s successfully.", *recordSetName)
		}

		sleepDuration := time.Duration(*interval) * time.Second
		log.Printf("Sleeping for %d seconds", sleepDuration)
		time.Sleep(sleepDuration)
	}
}
