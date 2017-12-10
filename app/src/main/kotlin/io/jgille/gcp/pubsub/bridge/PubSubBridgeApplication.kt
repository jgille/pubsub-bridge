package io.jgille.gcp.pubsub.bridge

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.*
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.jgille.gcp.pubsub.bridge.admin.PubSubAdminClient
import io.jgille.gcp.pubsub.bridge.config.PubSubProperties
import io.jgille.gcp.pubsub.bridge.config.PublishProperties
import io.jgille.gcp.pubsub.bridge.config.SubscribeProperties
import io.jgille.gcp.pubsub.bridge.logging.LoggingConfiguration
import io.jgille.gcp.pubsub.bridge.publish.PubSubDispatcherServlet
import io.jgille.gcp.pubsub.bridge.publish.PublishingDispatcher
import io.jgille.gcp.pubsub.bridge.publish.PublishingDispatcherImpl
import io.jgille.gcp.pubsub.bridge.subscribe.CloseableProxyClient
import io.jgille.gcp.pubsub.bridge.subscribe.ProxyClientsLifecycleManager
import io.jgille.gcp.pubsub.bridge.subscribe.ProxyMessageReceiver
import io.jgille.gcp.pubsub.bridge.subscribe.ProxySubscriber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.web.servlet.ServletRegistrationBean
import org.springframework.context.annotation.Bean
import org.threeten.bp.Duration

@SpringBootApplication
open class PubSubBridgeApplication {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(PubSubBridgeApplication::class.java, *args)
        }
    }

    @Bean
    open fun transportChannelProvider(properties: PubSubProperties): TransportChannelProvider {
        return properties.transportChannel()
    }

    @Bean
    open fun credentialsProvider(properties: PubSubProperties): CredentialsProvider {
        return properties.credentialsProvider()
    }

    @Bean
    open fun topicAdminClient(channelProvider: TransportChannelProvider,
                              credentialsProvider: CredentialsProvider): TopicAdminClient {
        val topicAdminSettings = TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build()
        return TopicAdminClient.create(topicAdminSettings)
    }

    @Bean
    open fun subscriptionAdminClient(channelProvider: TransportChannelProvider,
                                     credentialsProvider: CredentialsProvider): SubscriptionAdminClient {
        val subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build()
        return SubscriptionAdminClient.create(subscriptionAdminSettings)
    }

    @Bean
    open fun proxySubscribers(pubSubProperties: PubSubProperties,
                              subscribeProperties: SubscribeProperties,
                              loggingConfiguration: LoggingConfiguration,
                              channelProvider: TransportChannelProvider,
                              credentialsProvider: CredentialsProvider,
                              clientsLifecycleManager: ProxyClientsLifecycleManager): List<ProxySubscriber> {
        return subscribeProperties.subscriptions.map {
            val subscriptionName = SubscriptionName.of(pubSubProperties.projectId, it.subscription)
            val client = CloseableProxyClient(it.proxyTo!!)
            clientsLifecycleManager.manage(client)
            val concurrency = it.concurrency ?: subscribeProperties.defaultConcurrency
            logger.info("Setting up subscriber ${it.subscription} with $concurrency parallel pulls")
            val receiver = ProxyMessageReceiver(client, loggingConfiguration)
            val subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                    .setCredentialsProvider(credentialsProvider)
                    .setChannelProvider(channelProvider)
                    .setParallelPullCount(concurrency)
                    .build()
            ProxySubscriber(subscriber = subscriber)
        }
    }

    @Bean
    open fun dispatchers(pubSubProperties: PubSubProperties,
                         channelProvider: TransportChannelProvider,
                         credentialsProvider: CredentialsProvider,
                         publishProperties: PublishProperties,
                         adminClient: PubSubAdminClient): List<PublishingDispatcher> {
        val rootPath = publishProperties.rootPath
        return publishProperties.publishers.map {
            val topicName = TopicName.of(pubSubProperties.projectId!!, it.topic!!)
            val path = "$rootPath${it.path!!}"

            val timeout = it.timeout ?: publishProperties.defaultTimeout

            logger.info("Setting up publisher for topic ${it.topic} with $timeout ms timeout")

            val retrySettings = RetrySettings.newBuilder()
                    .setTotalTimeout(Duration.ofMillis(timeout))
                    .setInitialRetryDelay(Duration.ofMillis(5))
                    .setRetryDelayMultiplier(2.0)
                    .setMaxRetryDelay(Duration.ofMillis(Long.MAX_VALUE))
                    .setInitialRpcTimeout(Duration.ofSeconds(10))
                    .setRpcTimeoutMultiplier(2.0)
                    .setMaxRpcTimeout(Duration.ofSeconds(10))
                    .build()

            val publisher = Publisher.newBuilder(topicName)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .setRetrySettings(retrySettings)
                    .build()
            // TODO: Make this configurable
            val circuitBreaker = CircuitBreaker.of(it.path,
                    CircuitBreakerConfig.custom()
                            .ringBufferSizeInClosedState(100)
                            .ringBufferSizeInHalfOpenState(10)
                            .failureRateThreshold(100f)
                            .waitDurationInOpenState(java.time.Duration.ofSeconds(60))
                            .build())
            PublishingDispatcherImpl(path, publisher).protectedWith(circuitBreaker)
        }
    }

    @Bean
    open fun dispatcherRegistration(properties: PublishProperties,
                                    dispatcherServlet: PubSubDispatcherServlet): ServletRegistrationBean {
        val registration = ServletRegistrationBean(dispatcherServlet)
        registration.addUrlMappings("${properties.rootPath}/*")
        return registration
    }

}
