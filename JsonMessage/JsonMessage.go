package JsonMessage

import (
	"fmt"
	"strconv"
)

type JsonMessage struct {
	ApplicationId string        `json:"applicationid"`
	Api           interface{}   `json:"api"`
	Class         string        `json:"class"`
	TimeStamp     int           `json:"timeStamp"`
	Entity        interface{}   `json:"entity,omitempty"`
	Id            string        `json:"_id"`
	Method        string        `json:"method"`
	Fields        []interface{} `json:"fields,omitempty"`
}

func SetUserTable(classesName, appKey string) string {
	return fmt.Sprintf("ns:%s:%s:keys", classesName, appKey)
}
func HashUserTable(classesName, objectId, appKey string) string {
	return fmt.Sprintf("ns:%s:%s:%s:detail", classesName, objectId, appKey)
}
func CollectionTable(classesName, appKey string) string {
	return fmt.Sprintf("ns:%s:%s", classesName, appKey)
}
func FloatToString(input_num float64) string {
	return strconv.FormatFloat(input_num, 'f', 6, 64)
}
func IntToString(input_num int64) string {
	return strconv.FormatInt(input_num, 10)
}
func Setclasses(appkey string) string {
	return fmt.Sprintf("ns:classes:%s", appkey)
}
func Setschema(classes, appkey string) string {
	return fmt.Sprintf("ns:schema:%s:%s", classes, appkey)
}
