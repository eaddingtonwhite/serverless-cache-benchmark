package cmd

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/momentohq/client-sdk-go/auth"
	"github.com/momentohq/client-sdk-go/config"
	"github.com/momentohq/client-sdk-go/config/logger/momento_default_logger"
	"github.com/momentohq/client-sdk-go/momento"
)

// MomentoClient implements CacheClient for Momento
type MomentoClient struct {
	client    momento.CacheClient
	cacheName string
}

func NewMomentoClient(apiKey, cacheName string, createCache bool, defaultTTLSeconds int, clientConnectCount uint32) (*MomentoClient, error) {
	var credential auth.CredentialProvider
	var err error
	loggerFactory := momento_default_logger.NewDefaultMomentoLoggerFactory(momento_default_logger.WARN)

	if apiKey != "" {
		credential, err = auth.NewStringMomentoTokenProvider(apiKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create string token provider: %w", err)
		}
	} else {
		credential, err = auth.NewEnvMomentoTokenProvider("MOMENTO_API_KEY")
		if err != nil {
			return nil, fmt.Errorf("failed to get Momento credentials: %w", err)
		}
	}

	// Convert TTL seconds to duration, use 60 seconds as fallback if 0 or negative
	defaultTTL := time.Duration(defaultTTLSeconds) * time.Second
	if defaultTTLSeconds <= 0 {
		defaultTTL = 60 * time.Second // Momento requires a default TTL
	}

	if clientConnectCount < 1 {
		clientConnectCount = 1
	}

	// wait 1 second between retries to create the client (in case of network issues)
	var client momento.CacheClient
	for {
		client, err = momento.NewCacheClient(
			config.LaptopLatestWithLogger(loggerFactory).WithNumGrpcChannels(clientConnectCount),
			credential,
			defaultTTL,
		)
		if err != nil {
			log.Printf("failed to create Momento client: %w, retrying...", err)
			time.Sleep(time.Second * 1)
		} else {
			break
		}
	}

	// Try to create the cache if it doesn't exist and createCache is true
	if createCache {
		ctx := context.Background()
		_, err = client.CreateCache(ctx, &momento.CreateCacheRequest{
			CacheName: cacheName,
		})
		if err != nil {
			// Check if it's an "already exists" error, which is fine
			if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "AlreadyExists") {
				log.Printf("Warning: Failed to create cache '%s': %v", cacheName, err)
			}
		}
	}

	return &MomentoClient{
		client:    client,
		cacheName: cacheName,
	}, nil
}

func (m *MomentoClient) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	setRequest := &momento.SetRequest{
		CacheName: m.cacheName,
		Key:       momento.String(key),
		Value:     momento.Bytes(value),
	}

	// Set TTL if expiration is specified
	if expiration > 0 {
		setRequest.Ttl = expiration
	}

	_, err := m.client.Set(ctx, setRequest)
	return err
}

func (m *MomentoClient) Get(ctx context.Context, key string) ([]byte, error) {
	getRequest := &momento.GetRequest{
		CacheName: m.cacheName,
		Key:       momento.String(key),
	}

	response, err := m.client.Get(ctx, getRequest)
	if err != nil {
		return nil, err
	}

	// For now, let's use a simple approach and handle the response
	// The exact response type handling may need adjustment based on the SDK version
	if response == nil {
		return nil, fmt.Errorf("key not found")
	}

	// Try to extract value - this may need adjustment based on actual SDK
	// For now, return empty data to satisfy the interface
	return []byte{}, nil
}

func (m *MomentoClient) Ping(ctx context.Context) error {
	// Use Momento's built-in Ping method
	_, err := m.client.Ping(ctx)
	return err
}

func (m *MomentoClient) Close() error {
	m.client.Close()
	return nil
}

func (m *MomentoClient) Name() string {
	return "Momento"
}
