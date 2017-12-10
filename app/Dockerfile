FROM openjdk:8

WORKDIR /etc/pubsub-bridge

COPY build/libs/app.jar /opt/pubsub-bridge/bin/pubsub-bridge.jar
COPY config/sample.yml /etc/pubsub-bridge/pubsub-bridge.yml
COPY config/application.properties /etc/pubsub-bridge/application.properties

ENTRYPOINT ["java", "-Dspring.config.location=/etc/pubsub-bridge/pubsub-bridge.yml", "-jar", "/opt/pubsub-bridge/bin/pubsub-bridge.jar"]
