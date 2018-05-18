package radosgwapi_test

import (
	"encoding/json"
	"fmt"
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
	FuncName string      `json:"func_name"`
	ParaType string      `json:"para_type"`
	Para     interface{} `json:"para"`
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

func TestFunction(t *testing.T) {

	var result []reflect.Value
	for i, tc := range tests {

		switch tc.ParaType {
		case "string":
			result = reflectinvoker.InvokeByReflectArgs(tc.FuncName,
				[]reflect.Value{reflect.ValueOf(tc.Para)})
		case "ObjectConfig":
			objectConfigInCaseStr, _ := json.Marshal(tc.Para)
			objectConfigInCase := &ObjectConfigInCase{}
			json.Unmarshal(objectConfigInCaseStr, objectConfigInCase)

			var objectBody []byte
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
			}

			objectConfig := &radosgwapi.ObjectConfig{
				Bucket:       objectConfigInCase.Bucket,
				Key:          objectConfigInCase.Key,
				ObjectReader: strings.NewReader(string(objectBody)),
			}
			result = reflectinvoker.InvokeByReflectArgs(tc.FuncName,
				[]reflect.Value{reflect.ValueOf(objectConfig)})
		default:
			t.Error("unsupported para type:", tc.ParaType)
			continue
		}

		fmt.Println("usecase ", i,
			" -->  statusCode:", result[1].Int(),
			", httpContent:", string(result[0].Interface().([]byte)))

	}
}
