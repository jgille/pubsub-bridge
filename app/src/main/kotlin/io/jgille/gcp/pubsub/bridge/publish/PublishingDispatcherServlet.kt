package io.jgille.gcp.pubsub.bridge.publish

import io.github.resilience4j.circuitbreaker.CircuitBreakerOpenException
import io.jgille.gcp.pubsub.bridge.http.Headers
import io.jgille.gcp.pubsub.bridge.http.HttpHeader
import org.apache.http.HttpStatus
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.servlet.GenericServlet
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@Component
class PubSubDispatcherServlet(private val dispatchers: PublishingDispatchers) : GenericServlet() {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun service(req: ServletRequest, res: ServletResponse) {
        val httpRequest = req as HttpServletRequest
        val httpResponse = res as HttpServletResponse

        httpResponse.contentType = "application/json"

        val path = httpRequest.requestURI!!

        logger.info("Handling request on $path")

        val dispatcher = dispatchers.forPath(path)

        if (dispatcher == null) {
            logger.info("No dispatcher found for path $path")
            httpResponse.status = HttpStatus.SC_NOT_FOUND
            return
        }

        val method = httpRequest.method
        if (method != "POST") {
            logger.info("$path only supports POST, got $method")
            httpResponse.status = HttpStatus.SC_METHOD_NOT_ALLOWED
            return
        }

        val body: ByteArray = httpRequest.inputStream.use { it.readBytes() }
        val attributes = extractMessageAttributes(httpRequest)

        try {
            dispatcher.dispatch(body, attributes)
        } catch (e: TemporaryDispatchException) {
            logger.warn("Failed to publish message, could be a temporary glitch", e)
            httpResponse.status = HttpStatus.SC_SERVICE_UNAVAILABLE
        } catch (e: CircuitBreakerOpenException) {
            logger.warn("A circuit breaker is open", e)
            httpResponse.status = HttpStatus.SC_SERVICE_UNAVAILABLE
        } catch (e: Exception) {
            logger.error("Failed to publish message", e)
            httpResponse.status = HttpStatus.SC_INTERNAL_SERVER_ERROR
        }

        httpResponse.status = HttpStatus.SC_NO_CONTENT
    }

    private fun extractMessageAttributes(httpRequest: HttpServletRequest): Map<String, String> {
        val headers =
                httpRequest.headerNames.toList().map { HttpHeader(it, httpRequest.getHeader(it)) }
        val messageAttributes = Headers.toMessageAttributes(headers)
        return messageAttributes
    }

}

class PathMatcher {
    companion object {

        @JvmStatic
        fun pathMatch(p1: String, p2: String): Boolean {
            return removeTrailingSlash(p1) == removeTrailingSlash(p2)
        }

        @JvmStatic
        private fun removeTrailingSlash(path: String): String {
            return if (path.endsWith("/")) path.substring(0, path.length - 1) else path
        }
    }
}