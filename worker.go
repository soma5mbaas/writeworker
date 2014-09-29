package main

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/extra/pool"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"runtime"
	"strconv"
)

type JsonMessage struct {
	ApplicationId string      `json:"applicationid"`
	Api           interface{} `json:"api"`
	Class         string      `json:"class"`
	TimeStamp     int         `json:"timeStamp"`
	Entity        interface{} `json:"entity,omitempty"`
	Id            string      `json:"_id"`
	Method        string      `json:"method"`
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		//panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func FloatToString(input_num float64) string {
	return strconv.FormatFloat(input_num, 'f', 6, 64)
}
func IntToString(input_num int64) string {
	return strconv.FormatInt(input_num, 10)
}
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	conn, err := amqp.Dial("amqp://user:pass@localhost:port/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		"write", // queue
		"",      // consumer   	consumer에 대한 식별자를 지정합니다. consumer tag는 로컬에 channel이므로, 두 클라이언트는 동일한 consumer tag를 사용할 수있다.
		false,   // autoAck    	false는 명시적 Ack를 해줘야 메시지가 삭제되고 true는 메시지를 빼면 바로 삭제
		false,   // exclusive	현재 connection에만 액세스 할 수 있으며, 연결이 종료 할 때 Queue가 삭제됩니다.
		false,   // noLocal    	필드가 설정되는 경우 서버는이를 published 연결로 메시지를 전송하지 않을 것입니다.
		false,   // noWait		설정하면, 서버는 Method에 응답하지 않습니다. 클라이언트는 응답 Method를 기다릴 것이다. 서버가 Method를 완료 할 수 없을 경우는 채널 또는 연결 예외를 발생시킬 것입니다.
		nil,     // arguments	일부 브로커를 사용하여 메시지의 TTL과 같은 추가 기능을 구현하기 위해 사용된다.
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	pool, err := pool.NewPool("tcp", "localhost:port", 1)
	if err != nil {
		failOnError(err, "Failed to NewPool")
	}

	for i := 0; i < 1; i++ {
		go func() {
			session, err := mgo.Dial("localhost:port,localhost:port,localhost:port")
			if err != nil {
				panic(err)
			}
			defer session.Close()

			for d := range msgs {
				//Decoding arbitrary data
				var m JsonMessage
				{
					r := json.Unmarshal([]byte(d.Body), &m)
					if err != nil {
						failOnError(r, "Failed to json.Unmarshal")
						continue
					}
				}
				session.
				//Substituting the values
				AppKey := m.ApplicationId
				ObjectId := m.Id
				ClassesName := m.Class
				conns, err := pool.Get()
				if err != nil {
					failOnError(err, "Failed to pool.Get()")
					continue
				}

				CollectionName := CollectionTable(ClassesName, AppKey)
				//MongoDB set
				session.SetMode(mgo.Monotonic, true)
				c := session.DB("haru").C(CollectionName)

				//insert User table(PK)
				UserValue := SetUserTable(ClassesName, AppKey)
				//insert Object table(row)
				ObjectValue := HashUserTable(ClassesName, ObjectId, AppKey)
				fmt.Println(m.Method)
				switch m.Method {
				case "create":
					Obj := m.Entity.(map[string]interface{})

					for k, v := range Obj {
						switch vv := v.(type) {
						case string:
							conns.Append("hset", ObjectValue, k, vv)
						case float64:
							conns.Append("hset", ObjectValue, k, FloatToString(vv))
						case int64:
							conns.Append("hset", ObjectValue, k, IntToString(vv))
						default:
							fmt.Println(k, v, ObjectValue)
						}
					}
					conns.Append("zadd", UserValue, m.TimeStamp, ObjectId)

					//MongoDB Insert
					err = c.Insert(m.Entity)
					failOnError(err, "Failed to mongodb insert")
				case "delete":
					//Redis Remove
					conns.Append("del", ObjectValue)
					conns.Append("zrem", UserValue, ObjectId)

					//MongoDB Remove
					err = c.Remove(bson.M{"_id": ObjectId})
					failOnError(err, "Failed to mongodb Remove")
				case "update":
					Obj := m.Entity.(map[string]interface{})

					for k, v := range Obj {
						switch vv := v.(type) {
						case string:
							conns.Append("hset", ObjectValue, k, vv)
						case float64:
							conns.Append("hset", ObjectValue, k, FloatToString(vv))
						case int64:
							conns.Append("hset", ObjectValue, k, IntToString(vv))
						default:
							fmt.Println(k, v, ObjectValue)
						}
					}
					conns.Append("zadd", UserValue, m.TimeStamp, ObjectId)

					//MongoDB Update
					colQuerier := bson.M{"_id": ObjectId}
					change := bson.M{"$set": m.Entity}
					err = c.Update(colQuerier, change)
					failOnError(err, "Failed to mongodb update")
				default:
					var err error
					fmt.Println(m.Method)
					failOnError(err, "Failed to m.Method is null")
				}

				{
					//Redis Pipelining execute
					r := conns.GetReply()
					if r.Err != nil {
						failOnError(r.Err, "Failed to Pipelining GetReply")
						continue
					}
					fmt.Println(r)
				}
				//Redis Connection pool return
				pool.Put(conns)
				//RabbitMQ Message delete
				d.Ack(false)
			}

		}()
	}

	<-forever
	pool.Empty()
	log.Printf("Done")

	os.Exit(0)
}
