package io.jgille.gcp.pubsub.bridge.blackbox

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.*
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import groovy.json.JsonSlurper
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.apache.commons.lang3.RandomStringUtils
import org.apache.http.HttpStatus
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class PublisherSpec extends Specification {

    @Shared
    private RESTClient serviceClient

    @Shared
    private Subscriber subscriber

    @Shared
    private Map<String, PubsubMessage> receivedMessages = new ConcurrentHashMap<>()

    def setupSpec() {
        def serviceHost = System.getProperty("PUB_SUB_BRIDGE_HOST", "localhost")
        def pubSubHost = System.getProperty("PUB_SUB_HOST", "localhost")

        serviceClient = new RESTClient("http://${serviceHost}:8080", ContentType.JSON)

        startTestSubscriber(pubSubHost)
    }

    def cleanupSpec() {
        subscriber.stopAsync().awaitTerminated(10, TimeUnit.SECONDS)
    }

    private void startTestSubscriber(pubSubHost) {
        def pubSubTarget = "${pubSubHost}:8538"
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(pubSubTarget).usePlaintext(true).build()
        def transportChannel = GrpcTransportChannel.newBuilder().setManagedChannel(channel).build()
        def channelProvider = FixedTransportChannelProvider.create(transportChannel)
        def credentialsProvider = NoCredentialsProvider.create()

        def subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build()
        def adminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)

        def dogTopic = TopicName.of("sample-project", "dog_topic")
        def subscriptionName =
                SubscriptionName.of("sample-project", RandomStringUtils.randomAlphabetic(8))

        adminClient.createSubscription(subscriptionName, dogTopic, PushConfig.getDefaultInstance(), 0)

        adminClient.close()

        MessageReceiver receiver = new MessageReceiver() {
            @Override
            void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                receivedMessages.put(message.getAttributesOrDefault("event_id", ""), message)
                consumer.ack()
            }
        }

        subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setCredentialsProvider(credentialsProvider)
                .setChannelProvider(channelProvider)
                .build()
        subscriber.startAsync().awaitRunning()
    }

    def "A message posted via http is published on PubSub"() {
        when:
        def eventId = UUID.randomUUID().toString()
        def res = postMessage("/dispatch/dogs",
                [breed: "Poodle"],
                [
                        "X-Message-Attribute-aggregate_type": "DOG",
                        "X-Message-Attribute-event_type": "DOG_CREATED",
                        "X-Message-Attribute-event_id": eventId,
                        "X-Message-Attribute-Correlation-Id": UUID.randomUUID().toString(),
                        "Foo": "Bar"
                ])

        then:
        res.status == 204

        and:
        def conditions = new PollingConditions(delay: 1, initialDelay: 1, timeout: 20)
        PubsubMessage message = null
        conditions.eventually {
            message = receivedMessages[eventId]
            assert message != null
        }

        and:
        def attributes = message.getAttributesMap()
        attributes.size() == 4
        attributes.event_id == eventId
        attributes.event_type == "DOG_CREATED"
        attributes.aggregate_type == "DOG"

        and:
        def data = message.data.toByteArray()
        def json = new JsonSlurper().parse(data)
        json == [breed: "Poodle"]
    }

    def "Http 404 is returned when trying to post a message to a path that is not configured"() {
        when:
        postMessage("/dispatch/cats",
                [breed: "Maine Coon"],
                [
                        "X-Message-Attribute-Correlation-Id": UUID.randomUUID().toString()
                ])

        then:
        def e = thrown(HttpResponseException)
        e.statusCode == HttpStatus.SC_NOT_FOUND
    }

    def "Http 405 is returned when trying post a message but using PUT instead of POST"() {
        when:
        putMessage("/dispatch/dogs",
                [breed: "Poodle"],
                [
                        "X-Message-Attribute-aggregate_type": "DOG",
                        "X-Message-Attribute-event_type": "DOG_CREATED",
                        "X-Message-Attribute-Correlation-Id": UUID.randomUUID().toString(),
                        "Foo": "Bar"
                ])

        then:
        def e = thrown(HttpResponseException)
        e.statusCode == HttpStatus.SC_METHOD_NOT_ALLOWED
    }

    private def postMessage(String path, Map<String, Object> message, Map<String, String> headers) {
        serviceClient.post(
                path: path,
                headers: headers,
                body: message
        )
    }

    private def putMessage(String path, Map<String, Object> message, Map<String, String> headers) {
        serviceClient.put(
                path: path,
                headers: headers,
                body: message
        )
    }

}
