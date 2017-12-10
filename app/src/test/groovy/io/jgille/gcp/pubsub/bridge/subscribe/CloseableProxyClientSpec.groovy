package io.jgille.gcp.pubsub.bridge.subscribe

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import groovy.json.JsonOutput
import io.jgille.gcp.pubsub.bridge.config.ConfiguredHeader
import io.jgille.gcp.pubsub.bridge.config.ProxyProperties
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import static com.github.tomakehurst.wiremock.client.WireMock.*
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig

class CloseableProxyClientSpec extends Specification {

    @Shared
    private WireMockServer wireMockServer

    @Shared
    private WireMock wireMock

    @Shared
    private ProxyProperties properties

    @Shared
    private CloseableProxyClient client

    def setup() {
        def port = 52190
        wireMockServer = new WireMockServer(wireMockConfig().port(port))
        wireMockServer.start()

        wireMock = new WireMock("localhost", port)

        properties = new ProxyProperties()
        properties.url = "http://localhost:${wireMockServer.port()}/events/{attribute:event_type}"
        properties.headers = [new ConfiguredHeader(name: "a", value: "b")]
        client = new CloseableProxyClient(properties)
    }

    def cleanup() {
        wireMockServer.stop()
        client.close()
    }


    def "A PubSub message is proxied via http post"() {
        setup:
        def json = JsonOutput.toJson([hello: "world"])
        def eventType = "dog_created"
        wireMock.register(post("/events/$eventType")
                .willReturn(aResponse().withStatus(204))
        )
        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(json, "UTF-8"))
                .putAttributes("event_type", eventType)
                .build()

        when:
        client.proxyMessage(message)

        then:
        wireMock.verifyThat(postRequestedFor(urlEqualTo("/events/$eventType"))
                .withRequestBody(equalToJson(json))
                .withHeader("x-message-attribute-event_type", equalTo(eventType))
                .withHeader("a", equalTo("b"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
        )
    }

    @Unroll
    def "Http #status generates a retryable exception"() {
        setup:
        def json = JsonOutput.toJson([hello: "world"])
        def eventType = "dog_created"
        wireMock.register(post("/events/$eventType")
                .willReturn(aResponse().withStatus(status))
        )
        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(json, "UTF-8"))
                .putAttributes("event_type", eventType)
                .build()

        when:
        client.proxyMessage(message)

        then:
        thrown(RetryableException)


        where:
        status << [466, 503, 504]
    }

    def "An IOException generates a retryable exception"() {
        setup:
        def props = new ProxyProperties()
        def port = 56771
        props.url = "http://localhost:$port/events/{attribute:event_type}"
        def notConnectedClient = new CloseableProxyClient(props)
        def json = JsonOutput.toJson([hello: "world"])
        def eventType = "dog_created"

        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(json, "UTF-8"))
                .putAttributes("event_type", eventType)
                .build()

        when:
        notConnectedClient.proxyMessage(message)

        then:
        thrown(RetryableException)
    }

    @Unroll
    def "Http #status generates a runtime exception"() {
        setup:
        def json = JsonOutput.toJson([hello: "world"])
        def eventType = "dog_created"
        wireMock.register(post("/events/$eventType")
                .willReturn(aResponse().withStatus(status))
        )
        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(json, "UTF-8"))
                .putAttributes("event_type", eventType)
                .build()

        when:
        client.proxyMessage(message)

        then:
        thrown(UnexpectedHttpStatusException)

        where:
        status << [302, 400, 500]
    }
}
