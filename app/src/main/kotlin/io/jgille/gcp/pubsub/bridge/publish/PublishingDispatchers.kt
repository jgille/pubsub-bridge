package io.jgille.gcp.pubsub.bridge.publish

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.jgille.gcp.pubsub.bridge.admin.PubSubAdminClient
import io.jgille.gcp.pubsub.bridge.config.PublishProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ExecutionException
import javax.annotation.PostConstruct

@Component
class PublishingDispatchers(private val dispatchers: List<PublishingDispatcher>) {

    fun forPath(path: String): PublishingDispatcher? {
        return dispatchers.find { it.handles(path) }
    }

}

interface PublishingDispatcher {
    fun dispatch(body: ByteArray, attributes: Map<String, String>)

    fun handles(path: String): Boolean
}

class PublishingDispatcherImpl(private val path: String,
                           private val publisher: Publisher): PublishingDispatcher {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun handles(path: String): Boolean {
        return PathMatcher.pathMatch(this.path, path)
    }

    override fun dispatch(body: ByteArray, attributes: Map<String, String>) {
        logger.info("Publishing message to ${publisher.topicName.topic}")
        val message = PubsubMessage.newBuilder().setData(ByteString.copyFrom(body))
                .putAllAttributes(attributes)
                .build()

        try {
            publisher.publish(message).get()
            logger.info("Message published")
        } catch (e: ExecutionException) {
            throw TemporaryDispatchException(e.cause ?: e)
        }
    }

    fun protectedWith(circuitBreaker: CircuitBreaker): ProtectedPublishingDispatcher {
        return ProtectedPublishingDispatcher(this, circuitBreaker)
    }

}

class ProtectedPublishingDispatcher(private val dispatcher: PublishingDispatcher,
                                    private val circuitBreaker: CircuitBreaker): PublishingDispatcher {

    override fun handles(path: String): Boolean {
        return dispatcher.handles(path)
    }

    override fun dispatch(body: ByteArray, attributes: Map<String, String>) {
        return circuitBreaker.executeSupplier {
            dispatcher.dispatch(body, attributes)
        }
    }
}

class TemporaryDispatchException(cause: Throwable) : RuntimeException(cause)

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