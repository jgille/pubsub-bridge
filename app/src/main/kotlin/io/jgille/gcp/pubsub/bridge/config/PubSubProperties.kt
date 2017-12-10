package io.jgille.gcp.pubsub.bridge.config

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.ServiceOptions
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import io.grpc.ManagedChannelBuilder
import org.jetbrains.annotations.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import org.threeten.bp.Duration

@Component
@ConfigurationProperties(prefix = "pubSub")
open class PubSubProperties {

    var emulator: String? = null

    @NotNull
    var projectId: String? = ServiceOptions.getDefaultProjectId()

    fun transportChannel(): TransportChannelProvider {
        return emulator?.let { emulatorChannelProvider(it) } ?: defaultChannelProvider()
    }

    fun credentialsProvider(): CredentialsProvider {
        return emulator?.let { noCredentialsProvider() } ?: defaultCredentialsProvider()
    }

    private fun defaultChannelProvider(): TransportChannelProvider {
        return SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
                .setMaxInboundMessageSize(20 * 1024 * 1024)
                .setKeepAliveTime(Duration.ofMinutes(5))
                .build()
    }

    private fun emulatorChannelProvider(emulatorHost: String): FixedTransportChannelProvider {
        val channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext(true).build()
        val transportChannel = GrpcTransportChannel.newBuilder().setManagedChannel(channel).build()
        return FixedTransportChannelProvider.create(transportChannel)
    }

    private fun noCredentialsProvider(): CredentialsProvider = NoCredentialsProvider.create()

    private fun defaultCredentialsProvider(): CredentialsProvider {
        return SubscriptionAdminSettings.defaultCredentialsProviderBuilder().build()
    }
}