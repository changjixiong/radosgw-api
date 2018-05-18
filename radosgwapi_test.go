package radosgwapi_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	radosgwapi "github.com/changjixiong/radosgw-api"
	"github.com/changjixiong/reflectinvoke"
	ini "gopkg.in/ini.v1"
)

type ObjectConfigInCase struct {
	Bucket     string
	Key        string
	ObjectPath string
}

type RadosgwapiTestCase struct {
	FuncName           string            `json:"func_name"`
	ParaType           string            `json:"para_type"`
	Para               interface{}       `json:"para"`
	AddCustomHeader    map[string]string `json:"add_customHeader"`
	DeleteCustomHeader map[string]string `json:"delete_customHeader"`
}

var conn *radosgwapi.Connection
var reflectinvoker *reflectinvoke.Reflectinvoker
var tests []*RadosgwapiTestCase

func init() {

	cfg, err := ini.Load("radosgwapi.ini")
	if nil != err {
		fmt.Println(err)
		os.Exit(1)
	}

	testCase, err := os.Open("radosgwapi_testcase.json")
	testCaseStr, err := ioutil.ReadAll(testCase)

	if nil != err {
		fmt.Println(err)
		os.Exit(1)
	}

	conn = radosgwapi.NewConnection(
		cfg.Section("server").Key("host").String(),
		cfg.Section("user").Key("accessKeyID").String(),
		cfg.Section("user").Key("secretAccessKey").String(),
		http.Header{
			"Accept-Encoding": []string{"identity"},
		})

	reflectinvoker = reflectinvoke.NewReflectinvoker()
	reflectinvoker.RegisterMethod(conn)

	useCases := []*RadosgwapiTestCase{}

	err = json.Unmarshal(testCaseStr, &useCases)
	if nil != err {
		fmt.Println(err)
	}

	tests = append(tests, useCases...)

}

func addCustomHeader(conn *radosgwapi.Connection, customHeader map[string]string) {

	if len(customHeader) <= 0 {
		return
	}

	for k, v := range customHeader {
		conn.AddCustomHeader(k, v)
	}
}

func clearCustomHeader(conn *radosgwapi.Connection, customHeader map[string]string) {

	if len(customHeader) <= 0 {
		return
	}

	for k, _ := range customHeader {
		conn.DeleteCustomHeader(k)
	}
}

func TestFunction(t *testing.T) {

	var result []reflect.Value
	for i, tc := range tests {

		switch tc.ParaType {
		case "string":
			addCustomHeader(conn, tc.AddCustomHeader)
			result = reflectinvoker.InvokeByReflectArgs(tc.FuncName,
				[]reflect.Value{reflect.ValueOf(tc.Para)})
			clearCustomHeader(conn, tc.AddCustomHeader)
		case "ObjectConfig":
			objectConfigInCaseStr, _ := json.Marshal(tc.Para)
			objectConfigInCase := &ObjectConfigInCase{}
			json.Unmarshal(objectConfigInCaseStr, objectConfigInCase)

			var objectBody []byte
			var objectReader io.Reader
			if "" != objectConfigInCase.ObjectPath {
				file, err := os.Open(objectConfigInCase.ObjectPath)
				if nil != err {
					t.Error(err)
					continue
				}

				objectBody, err = ioutil.ReadAll(file)
				if nil != err {
					t.Error(err)
					continue
				}

				objectReader = strings.NewReader(string(objectBody))
			}

			objectConfig := &radosgwapi.ObjectConfig{
				Bucket:       objectConfigInCase.Bucket,
				Key:          objectConfigInCase.Key,
				ObjectReader: objectReader,
			}
			addCustomHeader(conn, tc.AddCustomHeader)
			result = reflectinvoker.InvokeByReflectArgs(tc.FuncName,
				[]reflect.Value{reflect.ValueOf(objectConfig)})
			clearCustomHeader(conn, tc.AddCustomHeader)
		default:
			t.Error("unsupported para type:", tc.ParaType)
			continue
		}

		fmt.Println("usecase ", i,
			" -->  statusCode:", result[1].Int(),
			", httpContent:", string(result[0].Interface().([]byte)))

	}
}
