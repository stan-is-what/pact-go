//go:build consumer
// +build consumer

package grpc

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pact-foundation/pact-go/v2/examples/grpc/routeguide"
	"github.com/pact-foundation/pact-go/v2/log"
	message "github.com/pact-foundation/pact-go/v2/message/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestListFeatures(t *testing.T) {
	p, _ := message.NewSynchronousPact(message.Config{
		Consumer: "grpcconsumer",
		Provider: "grpcprovider",
		PactDir:  filepath.ToSlash(fmt.Sprintf("%s/../pacts", dir)),
	})
	log.SetLogLevel("DEBUG")

	dir, _ := os.Getwd()
	path := fmt.Sprintf("%s/routeguide/route_guide.proto", strings.ReplaceAll(dir, "\\", "/"))

	grpcInteraction := `{
		"pact:proto": "` + path + `",
		"pact:proto-service": "RouteGuide/ListFeatures",
		"pact:content-type": "application/protobuf",
		"request": {
			"lo": {
				"latitude": "matching(number, 400000000)",
				"longitude": "matching(number, -750000000)"
			},
			"hi": {
				"latitude": "matching(number, 420000000)",
				"longitude": "matching(number, -730000000)"
			}
		},
		"response": {
			"name": "notEmpty('Liberty Bell')",
			"location": {
				"latitude": "matching(number, 409146138)",
				"longitude": "matching(number, -746188906)"
			}
		}
	}`

	err := p.AddSynchronousMessage("Route guide - ListFeatures").
		Given("features exist in the given rectangle").
		UsingPlugin(message.PluginConfig{
			Plugin:  "protobuf",
			Version: "0.5.4",
		}).
		WithContents(grpcInteraction, "application/protobuf").
		StartTransport("grpc", "127.0.0.1", nil).
		ExecuteTest(t, func(transport message.TransportConfig, m message.SynchronousMessage) error {
			fmt.Println("gRPC transport running on", transport)

			conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", transport.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal("unable to communicate to grpc server", err)
			}
			defer conn.Close()

			c := routeguide.NewRouteGuideClient(conn)

			rect := &routeguide.Rectangle{
				Lo: &routeguide.Point{
					Latitude:  400000000,
					Longitude: -750000000,
				},
				Hi: &routeguide.Point{
					Latitude:  420000000,
					Longitude: -730000000,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := c.ListFeatures(ctx, rect)

			if err != nil {
				t.Fatal(err.Error())
			}

			featureCount := 0
			for {
				feature, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err.Error())
				}
				featureCount++
				assert.NotEmpty(t, feature.GetName())
				assert.NotNil(t, feature.GetLocation())
			}

			assert.Greater(t, featureCount, 0)

			return nil
		})

	assert.NoError(t, err)
}

func TestRecordRoute(t *testing.T) {
	p, _ := message.NewSynchronousPact(message.Config{
		Consumer: "grpcconsumer",
		Provider: "grpcprovider",
		PactDir:  filepath.ToSlash(fmt.Sprintf("%s/../pacts", dir)),
	})
	log.SetLogLevel("DEBUG")

	dir, _ := os.Getwd()
	path := fmt.Sprintf("%s/routeguide/route_guide.proto", strings.ReplaceAll(dir, "\\", "/"))

	grpcInteraction := `{
		"pact:proto": "` + path + `",
		"pact:proto-service": "RouteGuide/RecordRoute",
		"pact:content-type": "application/protobuf",
		"request": {
			"latitude": "matching(number, 406109563)",
			"longitude": "matching(number, -742186778)"
		},
		"response": {
			"point_count": "matching(number, 2)",
			"feature_count": "matching(number, 0)",
			"distance": "matching(number, 0)",
			"elapsed_time": "matching(number, 1)"
		}
	}`

	err := p.AddSynchronousMessage("Route guide - RecordRoute").
		Given("route recording is enabled").
		UsingPlugin(message.PluginConfig{
			Plugin:  "protobuf",
			Version: "0.5.4",
		}).
		WithContents(grpcInteraction, "application/protobuf").
		StartTransport("grpc", "127.0.0.1", nil).
		ExecuteTest(t, func(transport message.TransportConfig, m message.SynchronousMessage) error {
			fmt.Println("gRPC transport running on", transport)

			conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", transport.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal("unable to communicate to grpc server", err)
			}
			defer conn.Close()

			c := routeguide.NewRouteGuideClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := c.RecordRoute(ctx)

			if err != nil {
				t.Fatal(err.Error())
			}

			points := []*routeguide.Point{
				{Latitude: 406109563, Longitude: -742186778},
				{Latitude: 411733222, Longitude: -741923909},
			}

			for _, point := range points {
				if err := stream.Send(point); err != nil {
					t.Fatal(err.Error())
				}
			}

			summary, err := stream.CloseAndRecv()
			if err != nil {
				t.Fatal(err.Error())
			}

			assert.Equal(t, int32(2), summary.GetPointCount())
			assert.GreaterOrEqual(t, summary.GetFeatureCount(), int32(0))
			assert.GreaterOrEqual(t, summary.GetDistance(), int32(0))
			assert.Greater(t, summary.GetElapsedTime(), int32(0))

			return nil
		})

	assert.NoError(t, err)
}

func TestRouteChat(t *testing.T) {
	p, _ := message.NewSynchronousPact(message.Config{
		Consumer: "grpcconsumer",
		Provider: "grpcprovider",
		PactDir:  filepath.ToSlash(fmt.Sprintf("%s/../pacts", dir)),
	})
	log.SetLogLevel("DEBUG")

	dir, _ := os.Getwd()
	path := fmt.Sprintf("%s/routeguide/route_guide.proto", strings.ReplaceAll(dir, "\\", "/"))

	grpcInteraction := `{
		"pact:proto": "` + path + `",
		"pact:proto-service": "RouteGuide/RouteChat",
		"pact:content-type": "application/protobuf",
		"request": {
			"location": {
				"latitude": "matching(number, 0)",
				"longitude": "matching(number, 1)"
			},
			"message": "notEmpty('First message')"
		},
		"response": {
			"location": {
				"latitude": "matching(number, 0)",
				"longitude": "matching(number, 1)"
			},
			"message": "notEmpty('Echo: First message')"
		}
	}`

	err := p.AddSynchronousMessage("Route guide - RouteChat").
		Given("route chat is available").
		UsingPlugin(message.PluginConfig{
			Plugin:  "protobuf",
			Version: "0.5.4",
		}).
		WithContents(grpcInteraction, "application/protobuf").
		StartTransport("grpc", "127.0.0.1", nil).
		ExecuteTest(t, func(transport message.TransportConfig, m message.SynchronousMessage) error {
			fmt.Println("gRPC transport running on", transport)

			conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", transport.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal("unable to communicate to grpc server", err)
			}
			defer conn.Close()

			c := routeguide.NewRouteGuideClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := c.RouteChat(ctx)

			if err != nil {
				t.Fatal(err.Error())
			}

			note := &routeguide.RouteNote{
				Location: &routeguide.Point{
					Latitude:  0,
					Longitude: 1,
				},
				Message: "First message",
			}

			if err := stream.Send(note); err != nil {
				t.Fatal(err.Error())
			}

			if err := stream.CloseSend(); err != nil {
				t.Fatal(err.Error())
			}

			responseCount := 0
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err.Error())
				}
				responseCount++
				assert.NotNil(t, response.GetLocation())
				assert.NotEmpty(t, response.GetMessage())
			}

			assert.GreaterOrEqual(t, responseCount, 0)

			return nil
		})

	assert.NoError(t, err)
}
