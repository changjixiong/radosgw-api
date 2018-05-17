package radosgwapi

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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

func (conn *connection) ListAllMyBuckets() (body []byte, statusCode int, err error) {
	args := url.Values{}
	body, statusCode, err = conn.Request("GET", "", args)
	return
}

func (conn *connection) DeleteBucket(bucketName string) (body []byte, statusCode int, err error) {
	args := url.Values{}
	body, statusCode, err = conn.Request("DELETE", "/"+bucketName, args)
	return
}

func (conn *connection) GetBucket(bucketName string) {

	args := url.Values{}
	args.Add("format", "json")

	body, _, err := conn.Request("GET", "/"+bucketName, args)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("body", strings.Repeat("-", 32))
	fmt.Println(string(body))
}

func (conn *connection) GetUser(uid string) {

	args := url.Values{}
	args.Add("format", "json")
	args.Add("uid", uid)

	body, _, err := conn.Request("GET", "/admin/user", args)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("body", strings.Repeat("-", 32))
	fmt.Println(string(body))
}

func (conn *connection) Request(method, router string, args url.Values) (body []byte, statusCode int, err error) {

	url := fmt.Sprintf("%s%s", conn.Host, router)
	if len(args) > 0 {
		url += "?" + args.Encode()
	}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return
	}

	conn.AddHttpHeader(req)

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
	statusCode = resp.StatusCode

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	return
}

func (conn *connection) AddHttpHeader(request *http.Request) {
	request.Header.Add("Accept-Encoding", "identity")
}
