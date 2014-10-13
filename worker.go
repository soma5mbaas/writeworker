package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   100,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
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

type JsonMessage struct {
	ApplicationId string      `json:"applicationid"`
	Api           interface{} `json:"api"`
	Class         string      `json:"class"`
	TimeStamp     int         `json:"timeStamp"`
	Entity        interface{} `json:"entity,omitempty"`
	Id            string      `json:"_id"`
	Method        string      `json:"method"`
}

func setclasses(appkey string) string {
	return fmt.Sprintf("ns:classes:%s", appkey)
}
func setschema(classes string, appkey string) string {
	return fmt.Sprintf("ns:schema:%s:%s", classes, appkey)
}

/////////////////////////////////////////////////////////////
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
	runtime.GOMAXPROCS(runtime.NumCPU())

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

	p := newPool("stage.haru.io:6400", "")
	defer p.Close()

	session, err := mgo.Dial("14.63.166.21:40000")
	session.SetMode(mgo.Strong, false)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	for i := 0; i < 80; i++ {
		go func() {

			for d := range msgs {
				// Redis Connection pool
				// get prunes stale connections and returns a connection from the idle list or
				// creates a new connection.
				conns := p.Get()
				if conns.Err() != nil {
					//failOnError(conns.Err(), "Failed to Get at Redis Connection pool")
					fmt.Println("Failed to Get at Redis Connection pool", conns.Err())
					time.Sleep(100 * time.Millisecond)
					continue
				}

				fmt.Println("ActiveCount: ", p.ActiveCount())

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

				//MongoDB set
				c := session.DB("test2").C(CollectionName)
				conns.Send("MULTI")

				switch m.Method {
				case "create":
					//Redis Insert
					Obj := m.Entity.(map[string]interface{})
					for k, v := range Obj {
						switch vv := v.(type) {
						case string:
							conns.Send("hset", ObjectValue, k, vv)
						case float64:

							str := strconv.FormatFloat(vv, 'g', -1, 64)
							n, err := strconv.ParseInt(str, 10, 64)

							// fmt.Println("string: ", k)
							// if k == "createAt" || k == "updateAt" {
							// 	Obj[k] = time.Now()
							// }

							if err == nil {
								Obj[k] = int(n)
								conns.Send("hset", ObjectValue, k, IntToString(n))
							} else {
								conns.Send("hset", ObjectValue, k, FloatToString(vv))
							}
						default:
							fmt.Println("create default", k, vv, ObjectValue)
							conns.Send("hset", ObjectValue, k, vv)
						}
					}
					conns.Send("zadd", UserValue, m.TimeStamp, ObjectId)

					//MongoDB Insert
					err = c.Insert(Obj)
					if err != nil {
						failOnError(err, "Failed to mongodb insert")
					}
				case "delete":
					//MongoDB Remove
					err = c.Remove(bson.M{"_id": ObjectId})
					if err != nil {
						failOnError(err, "Failed to mongodb Remove")
					}

					//Redis Remove
					conns.Send("del", ObjectValue)
					conns.Send("zrem", UserValue, ObjectId)
					fmt.Println("delete")
				case "deleteClass":

					items, err := redis.Strings(conns.Do("ZRANGE", UserValue, 0, -1))

					for _, v := range items {
						ZRANGEValue := HashUserTable(ClassesName, v, AppKey)
						conns.Send("DEL", ZRANGEValue)
						//MongoDB Remove
						err = c.Remove(bson.M{"_id": v})
						failOnError(err, "Failed to mongodb range Remove")
					}

					classkey := setclasses(AppKey)
					schemakey := setschema(ClassesName, AppKey)

					conns.Send("SREM", classkey, ClassesName)
					conns.Send("DEL", schemakey)
					conns.Send("DEL", UserValue)

				case "deleteField":
				case "update":
					//MongoDB Update
					colQuerier := bson.M{"_id": ObjectId}
					change := bson.M{"$set": m.Entity}
					err = c.Update(colQuerier, change)
					if err != nil {
						failOnError(err, "Failed to mongodb update")
					}

					//Redis Update
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
				default:
					var err error
					fmt.Println(m.Method)
					failOnError(err, "Failed to m.Method is null")
				}

				{
					//Redis Pipelining execute
					_, err := conns.Do("EXEC")
					if err != nil {
						failOnError(err, "Failed to Redis Receive")
					}
				}
				//Close releases the resources used by the Redis connection pool.
				conns.Flush()
				err = conns.Close()
				if err != nil {
					fmt.Println("Redis connection pool Close err: ", err)
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
