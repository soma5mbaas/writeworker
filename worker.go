package main

import (
	"./JsonMessage"
	"./logger"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//create logger
	str := "logfile" + JsonMessage.IntToString(int64(os.Getpid()))
	logger.CreateLogger(str)
	defer logger.DropLogger()

	//connect to RabbitMQ
	conn, err := amqp.Dial("amqp://admin:admin@stage.haru.io:5672/")
	logger.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	logger.FailOnError(err, "Failed to open a channel")
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
	logger.FailOnError(err, "Failed to register a consumer")

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
					logger.FailOnError(conns.Err(), "Failed to Get at Redis Connection pool")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				//Decoding arbitrary data
				var m JsonMessage.JsonMessage
				{
					r := json.Unmarshal([]byte(d.Body), &m)
					if err != nil {
						logger.FailOnError(r, "Failed to json.Unmarshal")
						continue
					}
				}

				//Substituting the values
				AppKey := m.ApplicationId
				ObjectId := m.Id
				ClassesName := m.Class

				//Create User table(PK)
				UserValue := JsonMessage.SetUserTable(ClassesName, AppKey)
				//Create Object table(row)
				ObjectValue := JsonMessage.HashUserTable(ClassesName, ObjectId, AppKey)
				//Create Object CollectionName(Collection)
				CollectionName := JsonMessage.CollectionTable(ClassesName, AppKey)

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

							if err == nil {
								Obj[k] = int(n)
								conns.Send("hset", ObjectValue, k, JsonMessage.IntToString(n))
							} else {
								conns.Send("hset", ObjectValue, k, JsonMessage.FloatToString(vv))
							}
						default:
							conns.Send("hset", ObjectValue, k, vv)
						}
					}
					conns.Send("zadd", UserValue, m.TimeStamp, ObjectId)

					//MongoDB Insert
					err = c.Insert(Obj)
					logger.FailOnError(err, "Failed to mongodb insert")
				case "delete":
					//MongoDB Remove
					err = c.Remove(bson.M{"_id": ObjectId})
					logger.FailOnError(err, "Failed to mongodb Remove")

					//Redis Remove
					conns.Send("del", ObjectValue)
					conns.Send("zrem", UserValue, ObjectId)
					fmt.Println("delete")
				case "deleteClass":
					//multi 후에 zcard를 하면 무조건 0 리턴하기때문에 EXEC를 해준다.
					conns.Do("EXEC")

					num, _ := redis.Int(conns.Do("zcard", UserValue))

					for i = 500; i < (num + 500); i += 500 {

						items, err := redis.Strings(conns.Do("ZRANGE", UserValue, 0, i))

						for _, v := range items {
							conns.Send("MULTI")

							ZRANGEValue := JsonMessage.HashUserTable(ClassesName, v, AppKey)
							conns.Send("DEL", ZRANGEValue)

							//MongoDB Remove
							err = c.Remove(bson.M{"_id": v})
							logger.FailOnError(err, "Failed to mongodb range Remove")
						}

						{
							//Redis Pipelining execute
							_, err := conns.Do("EXEC")
							logger.FailOnError(err, "Failed to Redis Receive")
						}
					}
					classkey := JsonMessage.Setclasses(AppKey)
					schemakey := JsonMessage.Setschema(ClassesName, AppKey)

					conns.Send("MULTI")
					conns.Send("SREM", classkey, ClassesName)
					conns.Send("DEL", schemakey)
					conns.Send("DEL", UserValue)

				case "deleteFields":

					schemakey := JsonMessage.Setschema(ClassesName, AppKey)
					for _, v := range m.Fields {
						conns.Send("Hdel", schemakey, v)
						conns.Send("Hdel", ObjectValue, v)
						//MongoDB Update
						colQuerier := bson.M{"_id": ObjectId}
						change := bson.M{"$unset": map[string]string{v.(string): ""}}
						err = c.Update(colQuerier, change)
						logger.FailOnError(err, "Failed to mongodb update")
					}

				case "update":
					conns.Do("EXEC")

					//ObjectValue라는 key가 있어야지 Update한다.
					check, _ := redis.Int64(conns.Do("exists", ObjectValue))

					if check == 1 {
						conns.Send("MULTI")
						//Redis Update
						Obj := m.Entity.(map[string]interface{})
						fmt.Println("update")
						for k, v := range Obj {
							switch vv := v.(type) {
							case string:
								conns.Send("hset", ObjectValue, k, vv)
							case float64:
								if k == "createAt" {
									delete(Obj, k)
								} else {
									conns.Send("hset", ObjectValue, k, JsonMessage.FloatToString(vv))
								}
							case int64:
								conns.Send("hset", ObjectValue, k, JsonMessage.IntToString(vv))
							default:
								conns.Send("hset", ObjectValue, k, vv)
							}
						}
						conns.Send("zadd", UserValue, m.TimeStamp, ObjectId)

						//MongoDB Update
						colQuerier := bson.M{"_id": ObjectId}
						change := bson.M{"$set": Obj}
						err = c.Update(colQuerier, change)
						logger.FailOnError(err, "Failed to mongodb update")
					}
				default:
					var err error
					fmt.Println(m.Method)
					logger.FailOnError(err, "Failed to m.Method is null"+m.Method)
				}

				{
					//Redis Pipelining execute
					_, err := conns.Do("EXEC")
					logger.FailOnError(err, "Failed to Redis Receive")
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
	fmt.Printf("Worker is Dead.")

	os.Exit(0)
}
