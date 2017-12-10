package io.jgille.gcp.pubsub.bridge.blackbox

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import groovy.json.JsonOutput
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.mockserver.client.server.MockServerClient
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.mockserver.verify.VerificationTimes
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.TimeUnit

class SubscriberSpec extends Specification {

    @Shared
    private Publisher publisher

    @Shared
    private MockServerClient mockServer

    def setupSpec() {
        def mockServerHost = System.getProperty("MOCK_SERVER_HOST", "localhost")
        def pubSubHost = System.getProperty("PUB_SUB_HOST", "localhost")

        mockServer = new MockServerClient(mockServerHost, 1080)

        def pubSubTarget = "${pubSubHost}:8538"
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(pubSubTarget).usePlaintext(true).build()
        def transportChannel = GrpcTransportChannel.newBuilder().setManagedChannel(channel).build()
        def channelProvider = FixedTransportChannelProvider.create(transportChannel)
        def credentialsProvider = NoCredentialsProvider.create()

        def topic = TopicName.of("sample-project", "cat_topic")
        publisher = Publisher.newBuilder(topic)
                .setCredentialsProvider(credentialsProvider)
                .setChannelProvider(channelProvider)
                .build()
    }

    def cleanupSpec() {
        publisher.shutdown()
    }

    def cleanup() {
        mockServer.reset()
    }

    def "When a message is consumed it is proxied to the backend service"() {
        given:
        def message = newDog("Poodle")
        def request = expectedHttpPost(message)
        mockServer.when(request).respond(HttpResponse.response().withStatusCode(204))

        when: "A message is published"
        publisher.publish(message).get(5, TimeUnit.SECONDS)

        then: "The message is proxied to the backend via http post"
        def conditions = new PollingConditions(timeout: 10, delay: 1, initialDelay: 1)
        conditions.eventually {
            mockServer.verify(request)
        }
    }

    def "A message is re-queued if the backend service is unavailable"() {
        given:
        def message = newDog("Bulldog")
        def request = expectedHttpPost(message)
        mockServer.when(request).respond(HttpResponse.response().withStatusCode(503))

        when: "A message is published"
        publisher.publish(message).get(5, TimeUnit.SECONDS)

        then: "The message is proxied to the backend via http post"
        def conditions = new PollingConditions(timeout: 10, delay: 1, initialDelay: 1)
        conditions.eventually {
            mockServer.verify(request, VerificationTimes.atLeast(2))
        }
    }

    private static HttpRequest expectedHttpPost(PubsubMessage message) {
        HttpRequest.request()
                .withMethod("POST")
                .withBody(message.data.toStringUtf8())
                .withHeader("Content-Type", "application/json; charset=UTF-8")
                .withHeader("x-message-attribute-event_type", message.getAttributesOrThrow("event_type"))
                .withHeader("Correlation-Id", message.getAttributesOrThrow("correlation_id"))
                .withHeader("Api-Key", "key")
                .withPath("/cats/events/${message.getAttributesOrThrow("event_type")}".toString())
    }

    private static PubsubMessage newDog(String breed) {
        def cid = UUID.randomUUID().toString()
        return PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(JsonOutput.toJson([breed: breed]), "UTF-8"))
                .putAttributes("event_type", "dog_created")
                .putAttributes("correlation_id", cid)
                .build()
    }
}
