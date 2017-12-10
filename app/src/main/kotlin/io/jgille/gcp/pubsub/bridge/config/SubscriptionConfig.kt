package io.jgille.gcp.pubsub.bridge.config

import org.apache.http.HttpStatus
import org.hibernate.validator.constraints.NotBlank
import org.jetbrains.annotations.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import javax.validation.Valid
import javax.validation.constraints.Min

@Component
@ConfigurationProperties(prefix = "subscribe")
open class SubscribeProperties {

    @NotNull
    var subscriptions: MutableList<SubscriptionProperties> = mutableListOf()

    @NotNull
    @Min(1)
    var defaultConcurrency: Int = Runtime.getRuntime().availableProcessors()
}

class SubscriptionProperties {

    @NotBlank
    var topic: String? = null

    @NotBlank
    var subscription: String? = null

    @NotNull
    @Valid
    var proxyTo: ProxyProperties? = null

    @Min(1)
    var concurrency: Int? = null
}

class ProxyProperties {

    @NotBlank
    var url: String? = null

    @NotNull
    var headers: MutableList<ConfiguredHeader> = mutableListOf()

    @NotBlank
    var contentType: String = "application/json"

    @NotNull
    var connectTimeout: Int = 10000

    @NotNull
    var connectionRequestTimeout: Int = 5000

    @NotNull
    var socketTimeout: Int = 10000

    @NotNull
    var retryableStatusCodes: MutableList<Int> = mutableListOf(
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            HttpStatus.SC_GATEWAY_TIMEOUT,
            CustomHttpStatus.RETRY_ME
    )

}

class ConfiguredHeader {

    @NotBlank
    var name: String? = null

    @NotBlank
    var value: String? = null

}

class CustomHttpStatus {
    companion object {
        @JvmStatic
        val RETRY_ME: Int = 466
    }
}