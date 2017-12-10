package io.jgille.gcp.pubsub.bridge.config

import org.hibernate.validator.constraints.NotBlank
import org.jetbrains.annotations.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import javax.validation.Valid
import javax.validation.constraints.Min

@Component
@ConfigurationProperties(prefix = "publish")
open class PublishProperties {

    @NotBlank
    var rootPath: String? = "/dispatch"

    @NotNull
    @Valid
    var publishers: MutableList<PublisherEndpointProperties> = mutableListOf()

    @NotNull
    @Min(10000)
    var defaultTimeout: Long = 10000
}

class PublisherEndpointProperties {

    @NotBlank
    var path: String? = null

    @NotBlank
    var topic: String? = null

    @Min(10000)
    var timeout: Long? = null
}
