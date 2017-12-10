FROM openjdk:8

WORKDIR /

COPY build/libs/app.jar /opt/pubsub-bridge/bin/pubsub-bridge.jar
COPY config/sample.yml /etc/pubsub-bridge/pubsub-bridge.yml

ENTRYPOINT ["java", "-Dspring.config.location=/etc/pubsub-bridge/pubsub-bridge.yml", "-jar", "/opt/pubsub-bridge/bin/pubsub-bridge.jar"]
