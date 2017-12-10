# PubSub Bridge

Acts as a bridge between your service and Google Cloud PubSub. The service posts a message (using http) to the bridge, which in turn publishes it to PubSub. The bridge also consumes PubSub messages, and when a message is received it is delegated to the service using http post.

This decouples your service from the specifics of the transportation layer, which leads to more flexibility, less lock-in and easier testing.

## Running locally

```
./gradlew build
docker-compose build
docker-compose up
```

## Run the tests
```
./gradlew blackbox-test
```