package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
)

type Redis struct {
	RedisClient redis.Client
}

func NewRedis() Redis {
	var client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if client == nil {
		errors.New("Cannot run redis")
	}

	return Redis{
		RedisClient: *client,
	}
}

// publisher
type MessagePublisher struct {
	redisClient Redis
}

func NewMessagePubliser(redisClient Redis) *MessagePublisher {
	return &MessagePublisher{redisClient}
}

func (p *MessagePublisher) PublishMessages(ctx context.Context, message interface{}, queueName string) error {

	serializedMessage, error := json.Marshal(message)

	if error != nil {
		log.Printf("[%s] Failed to serialize message: %v", queueName, error)
	}

	var err = p.redisClient.RedisClient.Publish(queueName, serializedMessage).Err()

	return err
}

type MessageConsumer struct {
	redisClient  Redis
	subscription *redis.PubSub
}

// NewMessageConsumer creates a new instance of MessageConsumer.
func NewMessageConsumer(redis Redis) *MessageConsumer {
	return &MessageConsumer{
		redisClient: redis,
	}
}

// This Function takes queue names in an array and uses a switch statement to perform required logic for the queues
func (c *MessageConsumer) ConsumerMessages(ctx context.Context, queueNames []string) {
	for _, queueName := range queueNames {
		switch queueName {
		case "Test":
			// We will handle the go routines in the custom function
			go c.handleCustomType1Logic(ctx, queueName)
		default:
			log.Printf("[%s] Unsupported message type: %+v\n", queueName, queueName)
		}
	}
}

// handleCustomType1Logic initiates a goroutine to handle messages from the specified queue.
func (c *MessageConsumer) handleCustomType1Logic(ctx context.Context, queueName string) {

	// Create a cancellation context to gracefully stop the goroutine
	consumerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("[%s] Consumer started listening...\n", queueName)

	// Subscribe to the specified Redis channel
	c.subscription = c.redisClient.RedisClient.Subscribe(queueName)
	defer c.subscription.Close()

	// Obtain the channel for receiving messages
	channel := c.subscription.Channel()

	for {
		select {
		// Check if the main context is canceled to stop the goroutine
		case <-consumerCtx.Done():
			log.Printf("[%s] Consumer stopped listening...\n", queueName)
			return
			// Listen for incoming messages on the channel
		case msg := <-channel:
			var messageObj string
			// Deserialize the message payload
			err := json.Unmarshal([]byte(msg.Payload), &messageObj)
			if err != nil {
				log.Printf("[%s] Failed to deserialize message: %v", queueName, err)
				continue
			}

			// Continue with your logic here:

			fmt.Printf("[%s] Received message: %+v\n", queueName, messageObj)
		}
	}
}

// slack channel message
type slackmessage struct {
	OrgID     string `json:"orgid"`
	ChannelID string `json:"channelid"`
	User      string `json:"user"`
	Content   string `json:"content"`
}

var ctx = context.Background()
var queueName = "redis_pubsub_queue"
var redisClient = NewRedis()
var redisPublisher = NewMessagePubliser(redisClient)

// postAlbums adds an album from JSON received in the request body.
func postSlackMessage(c *gin.Context) {

	var newSlackMessage slackmessage

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newSlackMessage); err != nil {
		return
	}

	err := redisPublisher.PublishMessages(ctx, newSlackMessage, queueName)
	if err != nil {
		log.Printf("[%s] Failed to publish the mesage: %v", queueName, err)
	}

	c.IndentedJSON(http.StatusCreated, newSlackMessage)

}

func getSlackMessages(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, "{}")
}

func main() {
	router := gin.Default()
	router.POST("/slack-chat", postSlackMessage)
	router.POST("/slack-chat/:channelid", getSlackMessages)
	router.Run("localhost:8080")
}
