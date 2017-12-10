package io.jgille.gcp.pubsub.bridge.http

class Headers {

    companion object {
        private val HEADER_PREFIX = "x-message-attribute-"

        @JvmStatic
        fun fromMessageAttributes(attributes: Map<String, String>): List<HttpHeader> {
            return attributes.map { (key, value) -> toHttpHeader(key, value) }
        }

        @JvmStatic
        fun toMessageAttributes(headers: List<HttpHeader>): Map<String, String> {
            return headers.filter {
                it.name.toLowerCase().startsWith(HEADER_PREFIX)
            }.map {
                it.name.substring(HEADER_PREFIX.length) to it.value
            }.toMap()
        }

        private fun toHttpHeader(key: String, value: String): HttpHeader {
            if (key.toLowerCase() in setOf("correlation_id", "correlation-id")) {
                return HttpHeader("Correlation-Id", value)
            }
            return HttpHeader("$HEADER_PREFIX$key", value)
        }
    }
}

data class HttpHeader(val name: String, val value: String)