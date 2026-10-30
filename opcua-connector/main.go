// opcua-connector subscribes to simulated sensor variables on the OPC UA
// server (hosted by the Node-RED container) and republishes each value
// change into JetStream on the edge tier of the NATS leafnode hierarchy,
// where the SENSORS_EDGE stream captures sensors.>.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type reading struct {
	Item      string  `json:"item"`
	Value     float64 `json:"value"`
	Timestamp string  `json:"timestamp"`
	Source    string  `json:"source"`
}

type monitoredItem struct {
	name   string
	nodeID string
}

func main() {
	endpoint := getenv("OPCUA_ENDPOINT", "opc.tcp://opcua-server:4840")
	natsURL := getenv("NATS_URL", "nats://nats-edge:4222")
	subjectPrefix := getenv("NATS_SUBJECT_PREFIX", "sensors")

	items := []monitoredItem{
		{name: "temperature", nodeID: "ns=1;s=Temperature"},
		{name: "pressure", nodeID: "ns=1;s=Pressure"},
		{name: "rpm", nodeID: "ns=1;s=RPM"},
	}

	nc := connectNATS(natsURL)
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream init error: %v", err)
	}

	for {
		if err := run(endpoint, subjectPrefix, items, js); err != nil {
			log.Printf("opcua session ended: %v - reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func connectNATS(url string) *nats.Conn {
	for {
		nc, err := nats.Connect(url, nats.Name("opcua-connector"), nats.MaxReconnects(-1))
		if err == nil {
			log.Printf("connected to NATS at %s", url)
			return nc
		}
		log.Printf("waiting for NATS at %s: %v", url, err)
		time.Sleep(3 * time.Second)
	}
}

func run(endpoint, subjectPrefix string, items []monitoredItem, js jetstream.JetStream) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpoints, err := opcua.GetEndpoints(ctx, endpoint)
	if err != nil {
		return err
	}
	ep, err := opcua.SelectEndpoint(endpoints, "None", ua.MessageSecurityModeNone)
	if err != nil {
		return err
	}
	// The server may advertise its own container hostname; always dial
	// the endpoint we were actually given.
	ep.EndpointURL = endpoint

	c, err := opcua.NewClient(ep.EndpointURL,
		opcua.SecurityPolicy("None"),
		opcua.SecurityModeString("None"),
		opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeAnonymous),
		opcua.AuthAnonymous(),
	)
	if err != nil {
		return err
	}
	if err := c.Connect(ctx); err != nil {
		return err
	}
	defer c.Close(ctx)
	log.Printf("connected to OPC UA server at %s", endpoint)

	notifyCh := make(chan *opcua.PublishNotificationData)
	sub, err := c.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 1 * time.Second}, notifyCh)
	if err != nil {
		return err
	}
	defer sub.Cancel(ctx)

	handleToItem := make(map[uint32]monitoredItem)
	for i, item := range items {
		id, err := ua.ParseNodeID(item.nodeID)
		if err != nil {
			return err
		}
		handle := uint32(i + 1)
		req := opcua.NewMonitoredItemCreateRequestWithDefaults(id, ua.AttributeIDValue, handle)
		res, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, req)
		if err != nil {
			return err
		}
		if res.Results[0].StatusCode != ua.StatusOK {
			log.Printf("warning: monitor %s returned status %v", item.nodeID, res.Results[0].StatusCode)
		}
		handleToItem[handle] = item
	}
	log.Printf("subscribed to %d items", len(items))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-notifyCh:
			if !ok {
				return nil
			}
			if res.Error != nil {
				log.Printf("subscription notification error: %v", res.Error)
				continue
			}
			dcn, ok := res.Value.(*ua.DataChangeNotification)
			if !ok {
				continue
			}
			for _, mi := range dcn.MonitoredItems {
				item, ok := handleToItem[mi.ClientHandle]
				if !ok {
					continue
				}
				value, ok := toFloat(mi.Value.Value.Value())
				if !ok {
					continue
				}
				publish(js, subjectPrefix, item.name, value)
			}
		}
	}
}

func publish(js jetstream.JetStream, prefix, item string, value float64) {
	r := reading{
		Item:      item,
		Value:     value,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Source:    "opcua-connector",
	}
	data, err := json.Marshal(r)
	if err != nil {
		log.Printf("marshal error: %v", err)
		return
	}
	subject := prefix + "." + item
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := js.Publish(ctx, subject, data)
	if err != nil {
		log.Printf("publish error: %v", err)
		return
	}
	log.Printf("-> %s [%s #%d]: %s", subject, ack.Stream, ack.Sequence, data)
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	default:
		return 0, false
	}
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
