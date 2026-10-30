// nats-connector consumes sensor readings from the SENSORS_HUB JetStream
// stream on the hub tier (the far end of the edge -> gateway -> hub
// sourcing chain) through a durable consumer, and writes each one into
// InfluxDB as a point. A message is acked only once InfluxDB has it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type reading struct {
	Item      string  `json:"item"`
	Value     float64 `json:"value"`
	Timestamp string  `json:"timestamp"`
	Source    string  `json:"source"`
}

func main() {
	natsURL := getenv("NATS_URL", "nats://nats-hub:4222")
	streamName := getenv("NATS_STREAM", "SENSORS_HUB")
	consumerName := getenv("NATS_CONSUMER", "influxdb-writer")
	influxURL := getenv("INFLUX_URL", "http://influxdb:8086")
	influxToken := getenv("INFLUX_TOKEN", "")
	influxOrg := getenv("INFLUX_ORG", "synadia-demo")
	influxBucket := getenv("INFLUX_BUCKET", "sensors")

	client := influxdb2.NewClient(influxURL, influxToken)
	defer client.Close()
	writeAPI := client.WriteAPIBlocking(influxOrg, influxBucket)

	nc := connectNATS(natsURL)
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream init error: %v", err)
	}
	cons := createConsumer(js, streamName, consumerName)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		handle(writeAPI, msg)
	})
	if err != nil {
		log.Fatalf("consume error: %v", err)
	}
	defer cc.Stop()
	log.Printf("consuming %s/%s on %s, writing into influxdb bucket %q", streamName, consumerName, natsURL, influxBucket)

	select {}
}

// createConsumer waits for the stream (created by nats-box-hub) to exist,
// then creates or updates the durable pull consumer on it.
func createConsumer(js jetstream.JetStream, stream, name string) jetstream.Consumer {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
			Durable:       name,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckWait:       30 * time.Second,
		})
		cancel()
		if err == nil {
			return cons
		}
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			log.Printf("waiting for stream %s", stream)
		} else {
			log.Printf("consumer create error: %v", err)
		}
		time.Sleep(3 * time.Second)
	}
}

func handle(writeAPI api.WriteAPIBlocking, msg jetstream.Msg) {
	var r reading
	if err := json.Unmarshal(msg.Data(), &r); err != nil {
		log.Printf("bad message on %s: %v", msg.Subject(), err)
		msg.Term()
		return
	}

	ts := time.Now()
	if r.Timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, r.Timestamp); err == nil {
			ts = parsed
		}
	}

	point := influxdb2.NewPoint(
		"sensor_data",
		map[string]string{"item": r.Item, "source": r.Source},
		map[string]interface{}{"value": r.Value},
		ts,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := writeAPI.WritePoint(ctx, point); err != nil {
		log.Printf("influxdb write error: %v", err)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
	log.Printf("wrote %s=%v to influxdb", r.Item, r.Value)
}

func connectNATS(url string) *nats.Conn {
	for {
		nc, err := nats.Connect(url, nats.Name("nats-connector"), nats.MaxReconnects(-1))
		if err == nil {
			log.Printf("connected to NATS at %s", url)
			return nc
		}
		log.Printf("waiting for NATS at %s: %v", url, err)
		time.Sleep(3 * time.Second)
	}
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
