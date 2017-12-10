package io.jgille.gcp.pubsub.bridge.subscribe

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.jgille.gcp.pubsub.bridge.logging.LoggingConfiguration
import spock.lang.Specification

class ProxyMessageReceiverSpec extends Specification {

    def "A message is proxied and then acked"() {
        given:
        def client = Mock(ProxyClient)
        def receiver = new ProxyMessageReceiver(client, new LoggingConfiguration())
        def consumer = Mock(AckReplyConsumer)
        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom("hello", "UTF-8")).build()

        when:
        receiver.receiveMessage(message, consumer)

        then:
        1 * client.proxyMessage(message)
        1 * consumer.ack()
    }

    def "A message is nacked if a retryable exception occurs"() {
        given:
        def client = Mock(ProxyClient)
        def receiver = new ProxyMessageReceiver(client, new LoggingConfiguration())
        def consumer = Mock(AckReplyConsumer)
        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom("hello", "UTF-8")).build()

        client.proxyMessage(message) >> { throw new RetryableException() }

        when:
        receiver.receiveMessage(message, consumer)

        then:
        1 * consumer.nack()
    }

    def "A message is acked if an exception that is not retryable occurs"() {
        given:
        def client = Mock(ProxyClient)
        def receiver = new ProxyMessageReceiver(client, new LoggingConfiguration())
        def consumer = Mock(AckReplyConsumer)
        def message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom("hello", "UTF-8")).build()

        client.proxyMessage(message) >> { throw new RuntimeException() }

        when:
        receiver.receiveMessage(message, consumer)

        then:
        1 * consumer.ack()
    }
}
