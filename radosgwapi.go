package radosgwapi

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	awsauth "github.com/smartystreets/go-aws-auth"
)

type ObjectConfig struct {
	Bucket       string
	Key          string
	ObjectReader io.Reader
}

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

func (conn *connection) ListBuckets() (body []byte, statusCode int, err error) {
	args := url.Values{}
	body, statusCode, err = conn.Request("GET", "", args, nil)
	return
}

func (conn *connection) DeleteBucket(bucketName string) (body []byte, statusCode int, err error) {
	args := url.Values{}
	body, statusCode, err = conn.Request("DELETE", "/"+bucketName, args, nil)
	return
}

func (conn *connection) CreateBucket(bucketName string) (body []byte, statusCode int, err error) {
	args := url.Values{}
	body, statusCode, err = conn.Request("PUT", "/"+bucketName, args, nil)
	return
}

func (conn *connection) GetBucket(bucketName string) (body []byte, statusCode int, err error) {

	args := url.Values{}

	body, statusCode, err = conn.Request("GET", "/"+bucketName, args, nil)

	return
}

func (conn *connection) GetUser(uid string) (body []byte, statusCode int, err error) {

	args := url.Values{}
	args.Add("uid", uid)

	body, statusCode, err = conn.Request("GET", "/admin/user", args, nil)

	return
}

func (conn *connection) PutObject(objectCfg *ObjectConfig) (body []byte, statusCode int, err error) {

	args := url.Values{}

	body, statusCode, err = conn.Request("PUT", "/"+objectCfg.Bucket+"/"+objectCfg.Key, args, objectCfg.ObjectReader)

	return
}

func (conn *connection) Request(method, router string, args url.Values, io io.Reader) (body []byte, statusCode int, err error) {

	url := fmt.Sprintf("%s%s", conn.Host, router)
	if len(args) > 0 {
		url += "?" + args.Encode()
	}
	req, err := http.NewRequest(method, url, io)
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
