package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	marathon "github.com/gambol99/go-marathon"
)

const (
	WEIGHTED   = "weighted"
	ENUMERATED = "enumerated"
)

type appError struct {
	Error   error
	IsFatal bool
}

var host = flag.String("marathon-host", "http://marathon.mesos:8080", "HTTP endpoint of Marathon service")
var appId = flag.String("app-id", "marathon-lb", "Marathon app id of marathon-lb service")
var hostedZoneId = flag.String("hosted-zone-id", "", "Route53 Hosted Zone")
var recordSetName = flag.String("record-set", "marathon-lb.example.com", "Record set to update")
var recordSetType = flag.String("record-set-type", "weighted,enumerated", "Comma separated list of record set types: weighted, enumerated")
var adminHostPort = flag.String("admin-http-port", "8080", "http port for admin/health check")

var recordSetTypes map[string]string = map[string]string{}

func updateRecords(client marathon.Marathon) *appError {
	// Fetch running marathon-lb tasks
	app, err := client.Application(*appId)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch appId: %s from host: %s, reason: %v", *appId, *host, err)
		return &appError{
			Error:   errors.New(msg),
			IsFatal: true,
		}
	}

	taskIps := make(map[string]string)
	for _, task := range app.Tasks {
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
	// We sort by IP to prevent unnecessary re-ordering of records
	sortedTaskIps := []string{}
	for _, ip := range taskIps {
		sortedTaskIps = append(sortedTaskIps, ip)
	}
	sort.Strings(sortedTaskIps)

	for idx, ip := range sortedTaskIps {
		if recordSetTypes[WEIGHTED] != "" {
			record := &route53.ResourceRecord{
				Value: aws.String(ip),
			}
			recordIdentifier := "weighted-" + ip
			recordSet := &route53.ResourceRecordSet{
				Name:            recordSetName,
				Type:            aws.String(route53.RRTypeA),
				TTL:             aws.Int64(60),
				Weight:          aws.Int64(10),
				SetIdentifier:   &recordIdentifier,
				ResourceRecords: []*route53.ResourceRecord{record},
			}
			recordUpsert := &route53.Change{
				Action:            aws.String(route53.ChangeActionUpsert),
				ResourceRecordSet: recordSet,
			}
			log.Printf("Creating record set %s", recordSet)
			changes = append(changes, recordUpsert)
		}

		if recordSetTypes[ENUMERATED] != "" {
			record := &route53.ResourceRecord{
				Value: aws.String(ip),
			}
			parts := strings.SplitN(*recordSetName, ".", 2)

			if len(parts) != 2 {
				return &appError{
					Error:   fmt.Errorf("record-set-name must have at least one . separator for enumerated records"),
					IsFatal: true,
				}
			}

			recordSetName := fmt.Sprintf("%s-%d.%s", parts[0], idx+1, parts[1])
			recordSet := &route53.ResourceRecordSet{
				Name:            &recordSetName,
				Type:            aws.String(route53.RRTypeA),
				TTL:             aws.Int64(60),
				ResourceRecords: []*route53.ResourceRecord{record},
			}
			recordUpsert := &route53.Change{
				Action:            aws.String(route53.ChangeActionUpsert),
				ResourceRecordSet: recordSet,
			}
			log.Printf("Creating record set %s", recordSet)
			changes = append(changes, recordUpsert)
		}
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

	types := strings.Split(*recordSetType, ",")
	for _, recordSetType := range types {
		cleanedType := strings.ToLower(strings.TrimSpace(recordSetType))
		recordSetTypes[cleanedType] = cleanedType
	}

	client := &http.Client{}

	config := marathon.NewDefaultConfig()
	config.URL = *host
	config.HTTPClient = client
	config.EventsTransport = marathon.EventsTransportSSE

	marathonClient, err := marathon.NewClient(config)

	if err != nil {
		log.Fatalf("Error creating marathon client: %v", err)
	}

	events, err := marathonClient.AddEventsListener(marathon.EventIDStatusUpdate)

	if err != nil {
		log.Fatalf("Error subscribing to event bus: %v", err)
	}
	defer marathonClient.RemoveEventsListener(events)

	httpAddr := "0.0.0.0:" + *adminHostPort
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ok, err := marathonClient.Ping()
		if err != nil || !ok {
			http.Error(w, "NOT OK", http.StatusServiceUnavailable)
		} else {
			fmt.Fprintln(w, "OK")
		}
	})

	httpServer := &http.Server{
		Addr:         httpAddr,
		Handler:      mux,
		ReadTimeout:  1000 * time.Millisecond,
		WriteTimeout: 1000 * time.Millisecond,
	}
	httpServer.SetKeepAlivesEnabled(false)

	// Start HTTP server in background
	go func() {
		log.Printf("Starting HTTPServer: addr=%v", httpAddr)
		err := httpServer.ListenAndServe()
		log.Printf("HTTPServer exited: err=%v", err)
	}()

	// update records on startup and then only when we receive a status update event for our app
	for {
		err := updateRecords(marathonClient)
		if err != nil {
			if err.IsFatal {
				log.Fatalf("FATAL: %v", err.Error)
			} else {
				log.Printf("WARNING: %v", err.Error)
			}
		}

		sleepDuration := 1 * time.Second // Sleep to prevent hammering the route53 api
		time.Sleep(sleepDuration)
		for {
			update := <-events
			log.Printf("StatusUpdate Received: %v", update)
			statusUpdate, _ := update.Event.(marathon.EventStatusUpdate)
			if statusUpdate.AppID == *appId {
				break
			}
		}
	}
}
