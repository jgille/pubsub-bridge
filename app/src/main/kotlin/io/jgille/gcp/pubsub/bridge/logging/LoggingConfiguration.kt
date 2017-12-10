package io.jgille.gcp.pubsub.bridge.logging

import org.hibernate.validator.constraints.NotBlank
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import javax.validation.Valid

@Component
@ConfigurationProperties(prefix = "logging")
class LoggingConfiguration {

    @Valid
    var mdc: List<MessageAttributeToMDCProperty> = mutableListOf()
}

class MessageAttributeToMDCProperty {

    @NotBlank
    var attributeName: String? = null

    var mdcPropertyName: String? = null
}