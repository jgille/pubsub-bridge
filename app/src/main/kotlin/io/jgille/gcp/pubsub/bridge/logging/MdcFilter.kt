package io.jgille.gcp.pubsub.bridge.logging

import io.jgille.gcp.pubsub.bridge.http.Headers
import org.slf4j.MDC
import org.springframework.stereotype.Component
import javax.servlet.*
import javax.servlet.http.HttpServletRequest

@Component
class MdcFilter(private val loggingConfiguration: LoggingConfiguration): Filter {
    override fun destroy() {
    }

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val httpRequest = request as HttpServletRequest
        httpRequest.headerNames.toList()
                .filter {
                    it.startsWith(Headers.MESSAGE_ATTRIBUTE_PREFIX, ignoreCase = true)
                }
                .forEach {
                    val name = it.substring(Headers.MESSAGE_ATTRIBUTE_PREFIX.length)
                    val value = httpRequest.getHeader(it)
                    loggingConfiguration.mdc.find { it.attributeName.equals(name, ignoreCase = true) }?.apply {
                        MDC.put(mdcPropertyName ?: attributeName, value)
                    }
                }
        try {
            chain.doFilter(request, response)
        } finally {
            MDC.clear()
        }
    }

    override fun init(filterConfig: FilterConfig?) {
    }
}