package radosgwapi

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	awsauth "github.com/smartystreets/go-aws-auth"
)

type connection struct {
	Host            string
	AccessKeyID     string
	SecretAccessKey string
}

func NewConnection(host, accessKeyID, secretAccessKey string) *connection {

	return &connection{
		Host:            host,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
}

func (conn *connection) ListBuckets() {

	req, err := http.NewRequest("GET", conn.Host, nil)
	if err != nil {
		return
	}

	awsauth.SignS3(req, awsauth.Credentials{
		AccessKeyID:     conn.AccessKeyID,
		SecretAccessKey: conn.SecretAccessKey,
		Expiration:      time.Now().Add(1 * time.Minute)},
	)

	client := http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	statusCode := resp.StatusCode
	fmt.Println("statusCode:", statusCode)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	fmt.Println("body", strings.Repeat("-", 32))
	fmt.Println(string(body))
}

func (conn *connection) GetBucket(bucketName string) {
	req, err := http.NewRequest("GET", conn.Host+"/"+bucketName, nil)
	if err != nil {
		return
	}

	awsauth.SignS3(req, awsauth.Credentials{
		AccessKeyID:     conn.AccessKeyID,
		SecretAccessKey: conn.SecretAccessKey,
		Expiration:      time.Now().Add(1 * time.Minute)},
	)

	client := http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	statusCode := resp.StatusCode
	fmt.Println("statusCode:", statusCode)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	fmt.Println("body", strings.Repeat("-", 32))
	fmt.Println(string(body))
}
