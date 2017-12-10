package io.jgille.gcp.pubsub.bridge.subscribe

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.PubsubMessage
import io.jgille.gcp.pubsub.bridge.admin.PubSubAdminClient
import io.jgille.gcp.pubsub.bridge.config.SubscribeProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

class ProxySubscriber(private val subscriber: Subscriber) {

    fun start() {
        subscriber.startAsync().awaitRunning()
    }

    fun stop() {
        subscriber.stopAsync().awaitTerminated()
    }

}

class ProxyMessageReceiver(val client: ProxyClient) : MessageReceiver {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer) {
        try {
            client.proxyMessage(message)
            consumer.ack()
            logger.info("Message handled")
        } catch (e: RetryableException) {
            logger.warn("Failed to handle message, will retry it", e)
            consumer.nack()
        } catch (e: Throwable) {
            logger.error("Failed to handle message $message", e)
            consumer.ack()
        }
    }

}

@Component
class ProxySubscribersLifeCycleManager(private val subscribers: List<ProxySubscriber>,
                                       private val properties: SubscribeProperties,
                                       private val adminClient: PubSubAdminClient) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun startSubscribers() {
        properties.subscriptions.forEach {
            adminClient.createTopicIfNecessary(it.topic!!)
            adminClient.createSubscriptionIfNecessary(it.topic!!, it.subscription!!)
        }

        subscribers.forEach {
            it.start()
        }
        logger.info("***** Started ${subscribers.size} subscribers *****")
    }

    @PreDestroy
    fun stopSubscribers() {
        subscribers.forEach { it.stop() }
        logger.info("All subscribers stopped")
    }

}