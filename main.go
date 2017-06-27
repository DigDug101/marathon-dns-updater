package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type appError struct {
	Error   error
	IsFatal bool
}

var host = flag.String("marathon-host", "http://marathon.mesos:8080", "HTTP endpoint of Marathon service")
var appId = flag.String("app-id", "marathon-lb", "Marathon app id of marathon-lb service")
var hostedZoneId = flag.String("hosted-zone-id", "", "Route53 Hosted Zone")
var recordSetName = flag.String("record-set", "marathon-lb.example.com", "Record set to update")

func updateRecords(api *MarathonAPI) *appError {
	// Fetch running marathon-lb tasks
	app, err := api.getApp(*appId)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch appId: %s from host: %s, reason: %v", *appId, *host, err)
		return &appError{
			Error:   errors.New(msg),
			IsFatal: true,
		}
	}

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
	// if we can't find any running tasks at all for this app something is probably wrong
	if len(taskIps) == 0 {
		return &appError{
			Error:   errors.New(fmt.Sprintf("No running tasks found for appId: %s", *appId)),
			IsFatal: true,
		}
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

		return &appError{
			Error:   err,
			IsFatal: false,
		}
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

	return nil
}

func filterStatusUpdateEvents(in <-chan *Event, ctx context.Context) <-chan *StatusUpdate {
	out := make(chan *StatusUpdate)
	go func() {
		// Listen for status update events that affect our app
		for e := range in {
			if e.Type == StatusUpdateEvent {
				var update StatusUpdate
				if err := json.Unmarshal(e.Data, &update); err != nil {
					log.Printf("Error deserialized status update: %v", err)
					continue
				}
				log.Printf("Received StatusUpdateEvent: %v", update)
				if update.AppID == *appId {
					select {
					case <-ctx.Done():
						return
					case out <- &update:
						continue
					}
				}
			}
		}
	}()

	return out
}

func main() {
	flag.Parse()

	if *hostedZoneId == "" {
		log.Println("Hosted zone id is required")
		flag.Usage()
		os.Exit(1)
	}

	if !strings.HasPrefix(*appId, "/") {
		*appId = "/" + *appId
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	api := &MarathonAPI{
		Client: client,
		Host:   *host,
		Path:   "v2",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allEvents := make(chan *Event, 1)
	errs := make(chan *error)

	go func() {
		for {
			err := <-errs
			log.Printf("Error processing events: %v", *err)
		}
	}()

	err := api.getEvents(allEvents, errs, ctx)
	if err != nil {
		log.Fatalf("Error subscribing to event bus: %v", err)
	}
	statusUpdateEvents := filterStatusUpdateEvents(allEvents, ctx)
	// update records on startup and then only when we receive a status update event for our app
	for {
		err := updateRecords(api)
		if err != nil {
			if err.IsFatal {
				cancel()
				log.Fatalf("FATAL: %v", err.Error)
			} else {
				log.Printf("WARNING: %v", err.Error)
			}
		}

		sleepDuration := 1 * time.Second // Sleep to prevent hammering the route53 api
		time.Sleep(sleepDuration)
		update := <-statusUpdateEvents
		log.Printf("StatusUpdate Received: %v", update)
	}
}
