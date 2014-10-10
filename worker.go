package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	//"runtime"
	"strconv"
	"time"
)

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,
		MaxActive:   10,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("admin", password); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

var (
	redisServer   = flag.String("stage.haru.io", ":6400", "")
	redisPassword = flag.String("", "", "")
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
func FloatToString(input_num float64) string {
	return strconv.FormatFloat(input_num, 'f', 6, 64)
}
func IntToString(input_num int64) string {
	return strconv.FormatInt(input_num, 10)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Println("%s: %s", msg, err)
		//panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	//runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	conn, err := amqp.Dial("amqp://admin:admin@stage.haru.io:5672/")
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

	flag.Parse()
	p := newPool(*redisServer, *redisPassword)

	session, err := mgo.Dial("14.63.166.21:40000")
	session.SetMode(mgo.Monotonic, true)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	for i := 0; i < 200; i++ {
		go func() {
			p, err := redis.Dial("tcp", "stage.haru.io:6400")
			failOnError(err, "Failed to redis.Dial")
			defer p.Close()

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

				//Substituting the values
				AppKey := m.ApplicationId
				ObjectId := m.Id
				ClassesName := m.Class

				//Create User table(PK)
				UserValue := SetUserTable(ClassesName, AppKey)
				//Create Object table(row)
				ObjectValue := HashUserTable(ClassesName, ObjectId, AppKey)
				//Create Object CollectionName(Collection)
				CollectionName := CollectionTable(ClassesName, AppKey)

				// Redis Connection pool
				// get prunes stale connections and returns a connection from the idle list or
				// creates a new connection.
				conns := p.Get()
				defer conns.Close()
				//conns := p

				//MongoDB set
				c := session.DB("test2").C(CollectionName)

				conns.Send("MULTI")

				switch m.Method {
				case "create":
					Obj := m.Entity.(map[string]interface{})
					fmt.Println("create")
					for k, v := range Obj {
						switch vv := v.(type) {
						case string:
							conns.Send("hset", ObjectValue, k, vv)
						case float64:
							conns.Send("hset", ObjectValue, k, FloatToString(vv))
						case int64:
							conns.Send("hset", ObjectValue, k, IntToString(vv))
						default:
							fmt.Println(k, vv, ObjectValue)
						}
					}
					conns.Send("zadd", UserValue, m.TimeStamp, ObjectId)

					//MongoDB Insert
					err = c.Insert(m.Entity)
					if err != nil {
						failOnError(err, "Failed to mongodb insert")
					}
				case "delete":
					//Redis Remove
					conns.Send("del", ObjectValue)
					conns.Send("zrem", UserValue, ObjectId)
					fmt.Println("delete")
					//MongoDB Remove
					err = c.Remove(bson.M{"_id": ObjectId})
					failOnError(err, "Failed to mongodb Remove")
				case "update":
					Obj := m.Entity.(map[string]interface{})
					fmt.Println("update")
					for k, v := range Obj {
						switch vv := v.(type) {
						case string:
							conns.Send("hset", ObjectValue, k, vv)
						case float64:
							conns.Send("hset", ObjectValue, k, FloatToString(vv))
						case int64:
							conns.Send("hset", ObjectValue, k, IntToString(vv))
						default:
							fmt.Println(k, v, ObjectValue)
						}
					}
					conns.Send("zadd", UserValue, m.TimeStamp, ObjectId)

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
					_, err := conns.Do("EXEC")
					failOnError(err, "Failed to Redis Receive")
				}

				//RabbitMQ Message delete
				d.Ack(false)
			}

		}()
	}

	<-forever
	log.Printf("Worker is Dead.")

	os.Exit(0)
}
