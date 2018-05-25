package radosgwapi

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	awsauth "github.com/smartystreets/go-aws-auth"
)

type ObjectConfig struct {
	Bucket       string
	Key          string
	ObjectReader io.Reader
	PicSize      int
}

type Connection struct {
	Host            string
	AccessKeyID     string
	SecretAccessKey string
	customHeader    http.Header
}

func (conn *Connection) AddCustomHeader(key, value string) {
	conn.customHeader.Add(key, value)
}

func (conn *Connection) DeleteCustomHeader(key string) {
	conn.customHeader.Del(key)
}

func NewConnection(host, accessKeyID, secretAccessKey string, customHeader http.Header) *Connection {

	return &Connection{
		Host:            host,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		customHeader:    customHeader,
	}
}

func (conn *Connection) ListBuckets(bucketName string) (body []byte, statusCode int, err error) {
	args := url.Values{}
	statusCode, _, body, err = conn.Request("GET", "/"+bucketName, args, nil)
	return
}

func (conn *Connection) DeleteBucket(bucketName string) (body []byte, statusCode int, err error) {
	args := url.Values{}
	statusCode, _, body, err = conn.Request("DELETE", "/"+bucketName, args, nil)
	return
}

func (conn *Connection) CreateBucket(bucketName string) (body []byte, statusCode int, err error) {
	args := url.Values{}
	statusCode, _, body, err = conn.Request("PUT", "/"+bucketName, args, nil)
	return
}

func (conn *Connection) GetBucket(bucketName string) (body []byte, statusCode int, err error) {

	args := url.Values{}

	statusCode, _, body, err = conn.Request("GET", "/"+bucketName, args, nil)

	return
}

func (conn *Connection) GetUser(uid string) (body []byte, statusCode int, err error) {

	args := url.Values{}
	args.Add("uid", uid)

	statusCode, _, body, err = conn.Request("GET", "/admin/user", args, nil)

	return
}

func (conn *Connection) PutObject(objectCfg *ObjectConfig) (body []byte, statusCode int, err error) {
	args := url.Values{}

	statusCode, _, body, err = conn.Request("PUT", "/"+objectCfg.Bucket+"/"+objectCfg.Key, args, objectCfg.ObjectReader)

	return
}

func (conn *Connection) PutObjectByPic(objectCfg *ObjectConfig) (body []byte, statusCode int, err error) {
	args := url.Values{}

	statusCode, _, body, err = conn.Request("POST", "/"+objectCfg.Bucket+"/"+objectCfg.Key+"?uploads", args, nil)

	if nil != err {
		return
	}

	// fmt.Println("statusCode:", statusCode, "body", body)

	initiateMultipartUploadResult := &InitiateMultipartUploadResult{}
	err = xml.Unmarshal(body, initiateMultipartUploadResult)

	if nil != err {
		fmt.Println(err)
		return
	}

	responseHeader := http.Header{}
	Etags := []string{}

	//TODO: add content
	string1 := ""
	args.Add("partNumber", "1")
	args.Add("UploadId", initiateMultipartUploadResult.UploadId)
	statusCode, responseHeader, body, err = conn.Request("PUT", "/"+objectCfg.Bucket+"/"+objectCfg.Key, args, strings.NewReader(string1))

	if nil != err {
		fmt.Println(err)
		return
	}

	if nil != responseHeader["Etag"] {
		Etags = append(Etags, responseHeader["Etag"]...)
	}

	args.Del("partNumber")
	args.Del("UploadId")
	fmt.Println("statusCode1:", statusCode, "body1:", body)

	//TODO: add content
	string2 := ""
	args.Add("partNumber", "2")
	args.Add("UploadId", initiateMultipartUploadResult.UploadId)
	statusCode, responseHeader, body, err = conn.Request("PUT", "/"+objectCfg.Bucket+"/"+objectCfg.Key, args, strings.NewReader(string2))

	if nil != err {
		fmt.Println(err)
		return
	}

	if nil != responseHeader["Etag"] {
		Etags = append(Etags, responseHeader["Etag"]...)
	}
	args.Del("partNumber")

	string3 := ``

	for n, etag := range Etags {
		string3 += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n+1, etag)
	}

	string3 = fmt.Sprintf("<CompleteMultipartUpload>%s</CompleteMultipartUpload>", string3)

	statusCode, _, body, err = conn.Request("POST", "/"+objectCfg.Bucket+"/"+objectCfg.Key, args, strings.NewReader(string3))

	return
}

func (conn *Connection) Request(method, router string, args url.Values, io io.Reader) (statusCode int, header http.Header, body []byte, err error) {

	url := fmt.Sprintf("%s%s", conn.Host, router)
	if len(args) > 0 {
		url += "?" + args.Encode()
	}
	req, err := http.NewRequest(method, url, io)
	if err != nil {
		return
	}

	conn.addHttpHeader(req)

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
	header = resp.Header
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	return
}

func (conn *Connection) addHttpHeader(request *http.Request) {

	for key, values := range conn.customHeader {
		for _, v := range values {
			request.Header.Add(key, v)
		}
	}
}
