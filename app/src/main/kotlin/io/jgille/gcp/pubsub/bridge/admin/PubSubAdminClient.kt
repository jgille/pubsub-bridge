package io.jgille.gcp.pubsub.bridge.admin

import com.google.api.gax.rpc.ApiException
import com.google.api.gax.rpc.StatusCode
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import io.jgille.gcp.pubsub.bridge.config.PubSubProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.annotation.PreDestroy

@Component
class PubSubAdminClient(val topicAndSubscriptionCreator: TopicAndSubscriptionCreator) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @PreDestroy
    fun close() {
        topicAndSubscriptionCreator.close()
    }

    fun createTopicIfNecessary(topic: String) {
        try {
            topicAndSubscriptionCreator.createTopic(topic)
            logger.info("Created topic '$topic'")
        } catch (e: ApiException) {
            if (e.statusCode.code == StatusCode.Code.ALREADY_EXISTS) {
                logger.info("The topic '$topic' already exists")
            } else {
                throw e
            }
        }
    }

    fun createSubscriptionIfNecessary(topic: String, subscription: String) {
        try {
            topicAndSubscriptionCreator.createSubscription(topic, subscription)
            logger.info("Created subscription '$subscription' on topic '$topic'")
        } catch (e: ApiException) {
            if (e.statusCode.code == StatusCode.Code.ALREADY_EXISTS) {
                logger.info("The subscription '$subscription' already exists")
            } else {
                throw e
            }
        }
    }

    fun topicExists(topic: String): Boolean {
        return topicAndSubscriptionCreator.topicExists(topic)
    }
}

interface TopicAndSubscriptionCreator {

    fun createTopic(topic: String)

    fun createSubscription(topic: String, subscription: String)

    fun close()

    fun topicExists(topic: String): Boolean
}

@Component
class TopicAndSubscriptionCreatorImpl(private val properties: PubSubProperties,
                                      private val topicAdminClient: TopicAdminClient,
                                      private val subscriptionAdminClient: SubscriptionAdminClient) : TopicAndSubscriptionCreator {

    override fun topicExists(topic: String): Boolean {
        return topicAdminClient.getTopic(TopicName.of(properties.projectId!!, topic)) != null
    }

    override fun createTopic(topic: String) {
        val topicName = TopicName.of(properties.projectId!!, topic)
        topicAdminClient.createTopic(topicName)
    }

    override fun createSubscription(topic: String, subscription: String) {
        val topicName = TopicName.of(properties.projectId!!, topic)
        val subscriptionName = SubscriptionName.of(properties.projectId!!, subscription)
        subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0)
    }

    override fun close() {
        topicAdminClient.close()
        subscriptionAdminClient.close()
    }

}