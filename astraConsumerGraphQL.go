package main

import (
   "bytes"
   "encoding/json"
   "errors"
   "fmt"
   "io/ioutil"
   "log"
   "net/http"
   "os"
   "reflect"
   "strings"
   "context"
   "strconv"
   "github.com/apache/pulsar-client-go/pulsar"
   Util "mylib"
)

// getAstraToken ... 
//   get the token to access Astra database
func getAstraToken(astraDB string, astraRegion string, userPasswd Util.UserPasswd)(*string, error) {

   svcURL := "https://" + astraDB + "-" + astraRegion + ".apps.astra.datastax.com/api/rest/v1/auth"
   dataPayload, err := json.Marshal(userPasswd)
   resp, err := http.Post( svcURL,
                           "application/json; charset=utf-8",
                           bytes.NewBuffer(dataPayload) )
   if err != nil {
      log.Fatalln(err)
      return nil, err
   }

   defer resp.Body.Close()
   bodyBytes, _ := ioutil.ReadAll(resp.Body)
   bodyString := string(bodyBytes)
   httpCode := resp.StatusCode

   if httpCode >= 400 {
      errStr := fmt.Sprintf("Failed to make HTTP API call [%d]\n------------------------\n%s", httpCode, bodyString)
      return nil, errors.New(errStr)
   }

   // When success, bodyString should be in format: {"authToken": "token-value-string"}
   token := strings.Split(bodyString, ":")[1]
   token = strings.ReplaceAll(token, "}", "")
   token = strings.ReplaceAll(token, "\"", "")

   return &token, nil
}

// getAstraJSONPayloadGraphQL ...
// - HTTP payload for Stargate Astra GraphQL API
func getAstraJSONPayloadGraphQL(sensorData Util.SensorDataAstra)(string) {
   
   dataPayload := "{ \"query\": \"mutation { insertsensor_data ( value: { "

   fields := reflect.TypeOf(sensorData)
   values := reflect.ValueOf(sensorData)
   num := fields.NumField()

   sensoridFieldname := strings.ToLower(fields.Field(0).Name)

   for j:=0; j<num; j++ {
      field := fields.Field(j)
      value := values.Field(j)

      dataPayload = dataPayload + strings.ToLower(field.Name) + ": "

      switch value.Kind() {
      case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
         dataPayload = dataPayload + strconv.FormatInt(value.Int(), 10)
      case reflect.String:
         dataPayload = dataPayload + "\\\"" + value.String() + "\\\""  
      case reflect.Float32, reflect.Float64:
         dataPayload = dataPayload + fmt.Sprintf("%f", value.Float()) 
   }

      if j<num-1 {
         dataPayload = dataPayload + ", "
      }
   }

   dataPayload = dataPayload + " } ) { value { " + sensoridFieldname + " } } }\" }"

   return dataPayload
}

// writeSensorDataToAstra ...
//   write sensor read from Pulsar to Astra using Stargate API
func writeSensorDataToAstra(
   astraDB string, astraRegion string, astraToken string,
   astraKs string, astraTbl string, sensorData Util.SensorDataAstra)(error) {

   svcURL := "https://" + astraDB + "-" + astraRegion + ".apps.astra.datastax.com/api/graphql/" + astraKs

   dataPayload := getAstraJSONPayloadGraphQL(sensorData)
   // fmt.Println(dataPayload)

   req, err := http.NewRequest(http.MethodPost, svcURL, bytes.NewBuffer([]byte(dataPayload)))
   if err != nil {
      log.Fatalln(err)
      return err
   }

   req.Header.Set("Content-Type", "application/json; charset=utf-8")
   req.Header.Set("X-Cassandra-Token", astraToken)

   client := &http.Client{}
   resp, err := client.Do(req)
   if err != nil {
      log.Fatalln(err)
   }

   defer resp.Body.Close()

   bodyBytes, _ := ioutil.ReadAll(resp.Body)
   bodyString := string(bodyBytes)
   httpCode := resp.StatusCode

   if httpCode >= 400 {
      errStr := fmt.Sprintf("Failed to make HTTP API call [%d]\n------------------------\n%s", httpCode, bodyString)
      return errors.New(errStr)
   }

   // When success, bodyString should be in format: {"succes": true}
   fmt.Println("     sensor data is successfully written Astra!")
   return nil
}


func main() {

   /**
   * Read configurations that are needed to connecto to Pulsar and Astra Stargate
   */
   fmt.Println("   Load Pulsar connection properties ...")
   props, err := Util.ReadPropertiesFile("./config.properties")
   if err != nil {
      log.Fatalln(err)
      fmt.Println("   > failed to load connection properties")
      os.Exit(10)
   }

   /**
   * Get Astra connection info
   */
   astraDB := props["astra_db_id"]
   astraRegion := props["astra_region"]

   // get the token to connect to Astra Stargate
   fmt.Println("   Get Astra connection token through Stargate API ...")
   astraAuth := Util.UserPasswd{ props["astra_username"], props["astra_password"] }
   astraToken, err := getAstraToken(astraDB, astraRegion, astraAuth)
   if (err != nil) {
      log.Fatalln(err)
      fmt.Println("   > failed to get Astra token")
      os.Exit(20)
   }
   // fmt.Println("Astra Token: " + *astraToken)

   /**
   * Get Pulsar cluster (Kesque) connection info
   */
   pulsarTokenStr := props["pulsar_token"]
   pulsarSvcURI := props["pulsar_svc_uri"]
   pulsarCertFileName := props["pulsar_trust_cert"]
   pulsarTopic := props["pulsar_topicName"]
   pulsarSubscription := props["pulsar_subscriptionName"]

   pulsarToken := pulsar.NewAuthenticationToken(pulsarTokenStr)
   
   fmt.Println("   Initiating Pulsar client ...")
   pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
      URL:                   pulsarSvcURI,
      Authentication:        pulsarToken,
      TLSTrustCertsFilePath: pulsarCertFileName,
   })
   if err != nil {
      log.Fatal(err)
      fmt.Println("   > failed to initialize Pulsar client")
      os.Exit(30)
   }

   defer pulsarClient.Close()

   fmt.Println("   Creating Pulsar consumer and subscription ...")
   sensorDataSchema := pulsar.NewAvroSchema(Util.SensorDataAvroSchema, nil)
   consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
      Topic:            pulsarTopic,
      SubscriptionName: pulsarSubscription,
      Schema: sensorDataSchema,
   })
   if err != nil {
      log.Fatal(err)
      fmt.Println("   > Failed to create Pulsar consumer and subscription")
      os.Exit(40)
   }

   defer consumer.Close()

   ctx := context.Background()

   // Write the sensor data that is retrieved from Pulsar into Astra
   astraKs := props["astra_keyspace"]
   astraTbl := props["astra_table"]
   cqlTimeStampFormatStr := "2006-01-02T15:04:05.000Z"

   sensorDataRaw := Util.SensorDataRaw{}

   // infinite loop to receive messages
   fmt.Println("   Receiving messages from Pulsar ...")
   for {
      msg, err := consumer.Receive(ctx)
      if err != nil {
         log.Fatal(err)
         fmt.Printf("   > failed to receive message : %v\n", string(msg.Payload()))
      } else {
         fmt.Printf("   > received message : %v", string(msg.Payload()))

         err = msg.GetSchemaValue(&sensorDataRaw)
         
         sensorid := sensorDataRaw.SensorID
         readingtime := sensorDataRaw.ReadingTime
         sensorytype := sensorDataRaw.SensorType
         readingvalue := sensorDataRaw.ReadingValue

         sensorDataAstra := Util.SensorDataAstra{
            SensorID:      sensorid.String(),
            DayOfYear:     readingtime.YearDay(),
            ReadingTime:   readingtime.Format(cqlTimeStampFormatStr),
            SensorType:    sensorytype,
            ReadingValue:  readingvalue, 
         }
         // fmt.Println(sensorDataAstra)

         err = writeSensorDataToAstra(astraDB, astraRegion, *astraToken, astraKs, astraTbl, sensorDataAstra)
         if (err != nil) {
               log.Fatalln(err)
               fmt.Println("   > inserting sensor data to Astra failed")
         }

         consumer.Ack(msg)
      }
   }


}
