package main

import (
   "context"
   "fmt"
   "log"
   "os"
   "time"
   "math/rand"
   "github.com/google/uuid"
   "github.com/apache/pulsar-client-go/pulsar"
   Util "mylib"
)

// Note: relace JWT token, tenant, namespace, and topic
func main() {

   fmt.Println("Pulsar Producer")

   /**
   * Read configurations that are needed to connecto to Pulsar and Astra Stargate
   */
   fmt.Println("   Load Pulsar connection properties ...")
   props, err := Util.ReadPropertiesFile("./config.properties")
   if err != nil {
      log.Fatalln(err)
      fmt.Println("   > Failed to load property files: config.properties")
      os.Exit(10)
   }

   /**
   * Get Pulsar cluster (Kesque) connection info
   */
   pulsarTokenStr := props["pulsar_token"]
   pulsarSvcURI := props["pulsar_svc_uri"]
   pulsarCertFileName := props["pulsar_trust_cert"]
   pulsarTopic := props["pulsar_topicName"]

   pulsarToken := pulsar.NewAuthenticationToken(pulsarTokenStr)

   fmt.Println("   Initiating Pulsar client ...")
   pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
      URL:                     pulsarSvcURI,
      Authentication:          pulsarToken,
      TLSTrustCertsFilePath:   pulsarCertFileName})
   if err != nil {
      log.Fatal(err)
      fmt.Println("   > Failed to initiate Pulsar cluster!")
      os.Exit(20)
   }

   defer pulsarClient.Close()

   fmt.Println("   Creating Pulsar producer ...")
   sensorDataSchema := pulsar.NewAvroSchema(Util.SensorDataAvroSchema, nil)
   producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
      Topic:  pulsarTopic,
      Schema: sensorDataSchema,
   })
   if err != nil || producer == nil {
      if err != nil {
         log.Fatal(err)
      }
      fmt.Println("   > Failed to create a Pulsar producer")
      os.Exit(30)
   }

   defer producer.Close()

   ctx := context.Background()

   // Send 10 messages synchronously
   fmt.Println("   Publishing messages ...")
   for i := 0; i < 10; i++ {
      
      // a random float value between 0 and 100
      rndmVal := rand.Float32() * 100

      sensorData := Util.SensorDataRaw{
         SensorID: uuid.New(),
         ReadingTime: time.Now(),
         SensorType: "temperature",
         ReadingValue: rndmVal }

      // Create a message
      msg := pulsar.ProducerMessage{
         // Payload: []byte(fmt.Sprintf("messageId-%d", i)),
         Value: sensorData,
      }
      // Attempt to send the message
      if _, err := producer.Send(ctx, &msg); err != nil {
         log.Fatal(err)
      } else {
         fmt.Printf("   > msg %s successfully published\n", string(msg.Payload))
      }

      // // Create a different message to send asynchronously
      // asyncMsg := pulsar.ProducerMessage{
      //   Payload: []byte(fmt.Sprintf("asyncMessageId-%d", i)),
      // }

      // // Attempt to send the message asynchronously and handle the response
      // producer.SendAsync(ctx, &asyncMsg, func(messageId pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
      //    if err != nil {
      //       log.Fatal(err)
      //    }

      //    fmt.Printf("   > msg \"%s\" successfully published\n", string(msg.Payload))
      // } )
   }
}
