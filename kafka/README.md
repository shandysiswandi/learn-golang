# KAFKA
before you run any command you have to change directory to this folder.
```sh
# if you in root project
cd kafka
```

## How to run `segmentio-kafka-go`

- consumer / subscriber
```sh
go run segmentio-kafka-go/main.go -consumer
# OR
go run segmentio-kafka-go/main.go --consumer
```

- producer / publisher
```sh
go run segmentio-kafka-go/main.go -producer
# OR
go run segmentio-kafka-go/main.go --producer
# with custom message
go run segmentio-kafka-go/main.go --producer --message=this-is-your-message
```