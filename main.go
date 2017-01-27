/*
A simple program to format a couple messages in avro and send them to
a MQTT server. Also tries to read back in order to provide some kind
of verification that it works.
*/
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	ais "github.com/andmarios/aislib"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/linkedin/goavro"
)

var (
	mqttServerAddr = flag.String("server", "localhost:1883", "MQTT server endpoint")
	originName     = flag.String("origin", "/ais", "origin queue or topic")
	logFilename    = flag.String("log", "", "file to write output (and logs), stdout if left empty")
	schemaFilename = flag.String("schema", "/classAPositionReport.schema", "file containing the avro schema to use")
	testMessages   = flag.Int("messages", 100000, "number of messages to send to kafka")
)

var workerWg sync.WaitGroup

const (
	numWorkers = 10
)

func main() {
	flag.Parse()

	if len(*logFilename) > 0 {
		logFile, err := os.OpenFile(*logFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening log file: %v\n", err)
		}
		defer logFile.Close()
		log.SetOutput(logFile)
	}

	// Load Schema File
	tempBuf, err := ioutil.ReadFile(*schemaFilename)
	if err != nil {
		log.Fatalf("Error opening schema file: %v\n", err)
	}
	schema := string(tempBuf)

	msgBus := make(chan ais.ClassAPositionReport, 100000)

	workerWg.Add(numWorkers)
	for i := 1; i <= numWorkers; i++ {
		go mqttWorker(msgBus, schema)
	}

	send := make(chan string, 1024*8)
	receive := make(chan ais.Message, 1024*8)
	failed := make(chan ais.FailedSentence, 1024*8)

	go ais.Router(send, receive, failed)

	nmeaData, err := os.Open("/nmea-sample")
	if err != nil {
		log.Fatalln(err)
	}
	defer nmeaData.Close()

	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		var message ais.Message
		numMessages := 0
		receive <- ais.Message{Type: 255}

	routerLoop:
		for {
			select {
			case message = <-receive:
				switch message.Type {
				case 1, 2, 3:
					t, _ := ais.DecodeClassAPositionReport(message.Payload)
					msgBus <- t
					numMessages++
					if numMessages == *testMessages {
						close(msgBus)
						break routerLoop
					}
				case 255:
					go func() {
						in := bufio.NewScanner(nmeaData)
						in.Split(bufio.ScanLines)

						for in.Scan() {
							send <- in.Text()
						}
						nmeaData.Seek(0, 0)
						receive <- ais.Message{Type: 255}
					}()
				default:
				}
			case <-failed:
			}
		}
	}()

	workerWg.Wait()
}

// mqttWorker receives ClassAPositionReport messages over a channel, encodes
// them to avro and sends them to mqtt
func mqttWorker(msgBus chan ais.ClassAPositionReport, schema string) {
	defer workerWg.Done()

	// Set Avro Codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatalln(err)
	}

	// Create an Avro Record
	record, err := goavro.NewRecord(goavro.RecordSchema(schema))
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Create a unique string for MQTT client id
	t := time.Now().UnixNano()
	uniqueText := fmt.Sprintf("%d", t)

	// Connect to ActiveMQ
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + *mqttServerAddr)
	opts.SetClientID("ais-source" + uniqueText)
	opts.SetCleanSession(true)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	defer client.Disconnect(250)

	var tempBuf *bytes.Buffer

	for msg := range msgBus {
		classA2Record(msg, record)
		tempBuf = new(bytes.Buffer)
		if err = codec.Encode(tempBuf, record); err != nil {
			log.Println(err)
		}
		token := client.Publish(*originName, byte(1), false, tempBuf.Bytes())
		token.Wait()
	}

}

// class2ARecord will take a ClassAPositionReport and set its values into an
// avro record
func classA2Record(m ais.ClassAPositionReport, r *goavro.Record) {
	r.Set("Type", int32(m.Type))
	r.Set("Repeat", int32(m.Repeat))
	r.Set("MMSI", int64(m.MMSI))
	r.Set("Speed", float32(m.Speed))
	r.Set("Accuracy", m.Accuracy)
	r.Set("Longitude", float64(m.Lon))
	r.Set("Latitude", float64(m.Lat))
	r.Set("Course", float32(m.Course))
	r.Set("Heading", int32(m.Heading))
	r.Set("Second", int32(m.Second))
	r.Set("RAIM", m.RAIM)
	r.Set("Radio", int64(m.Radio))
	r.Set("Status", int32(m.Status))
	r.Set("Turn", float32(m.Turn))
	r.Set("Maneuver", int32(m.Maneuver))
}
