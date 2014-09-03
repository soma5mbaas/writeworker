package main

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/streadway/amqp"
	"log"
	"os"
	"runtime"
	"time"
)

type JsonMessage struct {
	ApplicationId string
	UserID        string
	Api           map[string]string
	Class         string
	Method        string
	Object        map[string]string
}

func SetUserTable(_classesName string, _userId string, _appKey string) string {
	return fmt.Sprintf("ns:%s:%s:%s:keys", _classesName, _userId, _appKey)
}

func HashUserTable(_classesName string, _objectId string, _appKey string) string {
	return fmt.Sprintf("ns:%s:%s:%s:detail", _classesName, _objectId, _appKey)
}

func failOnError(_err error, _msg string) {
	if _err != nil {
		log.Fatalf("%s: %s", _msg, _err)
		panic(fmt.Sprintf("%s: %s", _msg, _err))
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	conn, err := amqp.Dial("amqp://admin:admin@stage.haru.io:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"write", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // noWait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	for i := 0; i < 100; i++ {
		go func() {
			//Variable Declaration
			var m JsonMessage
			var ClassesName string
			var AppKey string
			var UserId string
			var ObjectId string
			var UserValue string
			var ObjectValue string
			var r error

			//redis connetion
			c, err := redis.DialTimeout("tcp", "stage.haru.io:6379", time.Duration(10)*time.Second)
			failOnError(err, "Failed to redis connetion")
			defer c.Close()

			//select database
			RedisErr := c.Cmd("select", 9)
			failOnError(RedisErr.Err, "Failed to select database")

			for {
				for d := range msgs {
					//Decoding arbitrary data
					r = json.Unmarshal([]byte(d.Body), &m)
					failOnError(r, "Failed to json.Unmarshal")

					//Substituting the values
					ClassesName = m.Class
					AppKey = m.ApplicationId
					UserId = m.UserID
					ObjectId = m.Object["objectId"]

					//insert User table(PK)
					UserValue = SetUserTable(ClassesName, UserId, AppKey)
					r := c.Cmd("sadd", UserValue, ObjectId)
					failOnError(r.Err, "Failed to insert User table(PK)")

					//insert Object table(row)
					ObjectValue = HashUserTable(ClassesName, ObjectId, AppKey)
					r = c.Cmd("hset", ObjectValue, ObjectId, d.Body)
					failOnError(r.Err, "Failed to insert Object table(row)")

					d.Ack(false)
				}
			} //for
		}()
	}

	<-forever
	log.Printf("Done")

	os.Exit(0)
}
