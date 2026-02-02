package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"gostorelog/internal/entity"
)

// Client represents the storage client
type Client struct {
	baseURL string
}

// NewClient creates a new client
func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL}
}

// Publish publishes a record
func (c *Client) Publish(data interface{}, dataType int, partitionKey string) error {
	reqBody := map[string]interface{}{
		"data":          data,
		"data_type":     dataType,
		"partition_key": partitionKey,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	resp, err := http.Post(c.baseURL+"/publish", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("publish failed: %s", string(body))
	}
	return nil
}

// Read reads a record by partition and offset
func (c *Client) Read(partitionKey string, offset uint64) (*entity.Record, error) {
	url := fmt.Sprintf("%s/read?partition=%s&offset=%d", c.baseURL, partitionKey, offset)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("read failed: %s", string(body))
	}
	var record entity.Record
	if err := json.NewDecoder(resp.Body).Decode(&record); err != nil {
		return nil, err
	}
	return &record, nil
}