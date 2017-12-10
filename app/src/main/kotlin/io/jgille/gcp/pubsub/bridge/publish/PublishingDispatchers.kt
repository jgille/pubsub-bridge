package io.jgille.gcp.pubsub.bridge.publish

import com.google.api.gax.rpc.UnavailableException
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.jgille.gcp.pubsub.bridge.admin.PubSubAdminClient
import io.jgille.gcp.pubsub.bridge.config.PublishProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import javax.annotation.PostConstruct

@Component
class PublishingDispatchers(private val dispatchers: List<PublishingDispatcher>) {

    fun forPath(path: String): PublishingDispatcher? {
        return dispatchers.find { it.handles(path) }
    }

}

class PublishingDispatcher(private val path: String, val publisher: Publisher) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    fun handles(otherPath: String): Boolean {
        return PathMatcher.pathMatch(path, otherPath)
    }

    fun dispatch(body: ByteArray, attributes: Map<String, String>) {
        logger.info("Publishing message to ${publisher.topicName.topic}")
        val message = PubsubMessage.newBuilder().setData(ByteString.copyFrom(body))
                .putAllAttributes(attributes)
                .build()

        try {
            publisher.publish(message).get()
            logger.info("Message published")
        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is UnavailableException) {
                throw TemporaryDispatchException(cause)
            }
            throw cause ?: e
        } catch (e: TimeoutException) {
            throw TemporaryDispatchException(e)
        }
    }
}

class TemporaryDispatchException(cause: Exception): RuntimeException(cause)

@Component
class DispatcherLifecycleManager(private val properties: PublishProperties,
                                 private val adminClient: PubSubAdminClient) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun createTopics() {
        val publishers = properties.publishers
        logger.info("***** ${publishers.size} publishers configured *****")
        publishers.forEach {
            adminClient.createTopicIfNecessary(it.topic!!)
        }

    }
}