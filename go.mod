module github.com/kafkaesque-io/sample-pulsar-go-client

go 1.15

require (
	cloud.google.com/go/pubsub v1.9.0
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4
	github.com/apache/pulsar-client-go v0.3.0
	github.com/apache/pulsar/pulsar-client-go v0.0.0-20201112070640-c198b614d5fa
	github.com/google/gops v0.3.14
	github.com/google/uuid v1.1.2
	github.com/graphql-go/graphql v0.7.9 // indirect
	google.golang.org/api v0.36.0
	mylib v0.0.0
)

replace mylib => ./lib
