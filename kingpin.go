package main

import (
        "fmt"
        "encoding/json"
        "net/http"
        "log"
        "github.com/streadway/amqp"
        "github.com/gorilla/mux"
)

// Gorilla routes use this channel to speak to job queuer actor ..
var jobChannel chan string = make(chan string)

// Generic function for printing errors
func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

// RabbitMQ connection actor - instantiating at Main startup - recieves messages to queue
//
func queuerrrr() {

        log.Print("Connecting to RabbitMQ....")
        conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
        failOnError(err, "Failed to connect to RabbitMQ")
        defer conn.Close()

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")
        defer ch.Close()

        log.Print("Declaring content_crawler_queue")
        q, err := ch.QueueDeclare(
                "content_crawler_queue", // name
                true,    // durable
                false,   // delete when unused
                false,   // exclusive
                false,   // noWait
                nil,     // arguments
        )
        failOnError(err, "Failed to declare a queue")

        for {
                body := <-jobChannel

                err = ch.Publish(
                        "",           // exchange
                        q.Name, // routing key
                        false,        // mandatory
                        false,
                        amqp.Publishing {
                                DeliveryMode:  amqp.Persistent,
                                ContentType:     "text/plain",
                                Body:            []byte(body),
                        })
                failOnError(err, "Failed to publish a message")
                log.Print("Queueerrrr - published: ", body)
        }
}


// Gorilla stuff

func router() *mux.Router {
        router := mux.NewRouter()
        router.HandleFunc("/", IndexHandler).Methods("GET")
        router.HandleFunc("/job/{jobType}", JobHandler).Methods("POST")
        return router
}

func routerHandler(router *mux.Router) http.HandlerFunc {
        return func(res http.ResponseWriter, req *http.Request) {
          router.ServeHTTP(res, req)
          }
}

func IndexHandler(res http.ResponseWriter, req *http.Request) {
        log.Print("IndexHandler - hello Jimmy ")
        data, err := json.Marshal("{'hello':'Jimmy!'}")
        if err != nil {
          log.Fatal("Couldn't marshall json: ", err)
        }
        res.Header().Set("Content-Type", "application/json; charset=utf-8")
        res.Write(data)
}

func JobHandler(res http.ResponseWriter, req *http.Request) {
        vars := mux.Vars(req)
        jobType := vars["jobType"]
        log.Printf("JobHandler - %s", jobType)
        jobChannel <- jobType

        data, err := json.Marshal("{'JOB': 'TYPE!'}")
        if err != nil {
          log.Fatal("Couldn't marshall json: ", err)
        }
        res.Header().Set("Content-Type", "application/json; charset=utf-8")
        res.Write(data)
}

// Badman function
func main() {
        log.Print("Starting up Da Kingpin Of Jobs.. stand back!")

        go queuerrrr()

        log.Print("\nStarting Gorilla Router")
        handler := routerHandler(router())
        err := http.ListenAndServe(":8080", handler)

        failOnError(err, "Failed to start ListenAndServe")
}
