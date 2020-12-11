package lib

import (
	"bufio"
	"log"
	"os"
	"strings"
	"time"
	"github.com/google/uuid"
)

// UserPasswd ...
type UserPasswd struct {
	Username  string  `json:"username"`
	Password  string  `json:"password"`
} 

// SensorDataAstra ...
//   SensorData schema for Astra C*
type SensorDataAstra struct {
	SensorID      string
	DayOfYear     int
	ReadingTime   string
	SensorType    string
	ReadingValue  float32
}

// SensorDataRaw ...
//    SensorData schema for Pulsar messages
type SensorDataRaw struct {
	SensorID      uuid.UUID
	ReadingTime   time.Time
	SensorType    string
	ReadingValue  float32
}

// SensorDataAvroSchema ...
var SensorDataAvroSchema string = `{
	"type": "record",
	"name": "SensorData",
	"namespace": "TestNS",
	"fields" : [
		{"name": "SensorID", "type": "string", "logicalType": "UUID"},
		{"name": "ReadingTime", "type": "string", "logicalType": "time-micros"},
		{"name": "SensorType", "type": "string"},
		{"name": "ReadingValue", "type": "float"}
	]
}`

// ConfigProperties ...
type ConfigProperties map[string]string

// ReadPropertiesFile ...
func ReadPropertiesFile(filename string) (ConfigProperties, error) {
	config := ConfigProperties{}

	if len(filename) == 0 {
		return config, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if equal := strings.Index(line, "="); equal >= 0 {
			if key := strings.TrimSpace(line[:equal]); len(key) > 0 {
				value := ""
				if len(line) > equal {
					value = strings.TrimSpace(line[equal+1:])
				}
				config[key] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
		return nil, err
	}

	return config, nil
}
