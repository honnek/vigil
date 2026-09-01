package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	base  string
	token string
	http  *http.Client
}

func NewClient(base, token string) *Client {
	return &Client{
		base:  base,
		token: token,
		http:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) Alerts() ([]Alert, error) {
	var out []Alert
	return out, c.get("/alerts", &out)
}

func (c *Client) Logs(limit int) ([]Log, error) {
	var out []Log
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	return out, c.get("/logs?"+q.Encode(), &out)
}

func (c *Client) Series() ([]Series, error) {
	var out []Series
	return out, c.get("/series", &out)
}

func (c *Client) Metrics(host, name string, from, to time.Time, limit int64) ([]Metric, error) {
	var out []Metric
	q := url.Values{}
	q.Set("host", host)
	q.Set("name", name)
	q.Set("from", from.Format(time.RFC3339))
	q.Set("to", to.Format(time.RFC3339))
	q.Set("limit", strconv.FormatInt(limit, 10))

	return out, c.get("/metrics?"+q.Encode(), &out)
}

func Login(base, user, pass string) (*Client, error) {
	c := NewClient(base, "")

	creds, err := json.Marshal(map[string]string{"username": user, "password": pass})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.base+"/login", bytes.NewReader(creds))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("login failed: %s", resp.Status)
	}

	var out struct {
		Signed string `json:"signed"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	if err != nil {
		return nil, err
	}
	c.token = out.Signed

	return c, nil
}

func (c *Client) get(path string, out any) error {
	u := c.base + path
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", u, resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(out)
	if err != nil {
		return err
	}

	return nil
}
