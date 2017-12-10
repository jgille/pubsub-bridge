package io.jgille.gcp.pubsub.bridge.subscribe

import com.google.pubsub.v1.PubsubMessage
import io.jgille.gcp.pubsub.bridge.config.ProxyProperties
import io.jgille.gcp.pubsub.bridge.http.Headers
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.Closeable
import java.nio.charset.Charset
import javax.annotation.PreDestroy

interface ProxyClient : Closeable {

    fun proxyMessage(message: PubsubMessage)

}

class CloseableProxyClient(val properties: ProxyProperties) : ProxyClient {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val client: CloseableHttpClient

    private val retryableStatusCodes: List<Int> = properties.retryableStatusCodes

    init {
        val requestConfig = RequestConfig.custom()
                .setConnectTimeout(properties.connectTimeout)
                .setConnectionRequestTimeout(properties.connectionRequestTimeout)
                .setSocketTimeout(properties.socketTimeout)
                .build()

        client = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .setDefaultHeaders(properties.headers.map { BasicHeader(it.name!!, it.value!!) })
                .build()
    }

    override fun close() {
        client.close()
    }

    override fun proxyMessage(message: PubsubMessage) {
        val url = buildProxyUrl(message)
        logger.info("Proxying to $url")
        val request = buildRequest(url, message)
        try {
            post(request)
        } catch (e: java.io.IOException) {
            throw RetryableException(e)
        }
    }

    private fun buildRequest(url: String, message: PubsubMessage): HttpPost {
        val request = HttpPost(url)
        request.entity = ByteArrayEntity(message.data.toByteArray(),
                ContentType.create(properties.contentType, Charset.forName("UTF-8")))
        val headers = Headers.fromMessageAttributes(message.attributesMap)
        headers.forEach { request.addHeader(it.name, it.value)}
        return request
    }

    private fun buildProxyUrl(message: PubsubMessage): String {
        var url = properties.url!!
        message.attributesMap.forEach {
            val name = it.key
            url = url.replace("{attribute:$name}", it.value)
        }
        return url
    }

    private fun post(request: HttpPost) {
        client.execute(request).use {
            if (shouldRetry(it)) {
                throw ServiceUnavailableException()
            } else if (it.statusLine.statusCode > 299) {
                throw UnexpectedHttpStatusException("Got response status ${it.statusLine.statusCode}")
            }
        }
    }

    private fun shouldRetry(response: CloseableHttpResponse): Boolean {
        return response.statusLine.statusCode in retryableStatusCodes
    }

}

class UnexpectedHttpStatusException(msg: String): RuntimeException(msg)

@Component
class ProxyClientsLifecycleManager {

    private val clients: MutableList<ProxyClient> = mutableListOf()

    fun manage(client: ProxyClient) {
        clients.add(client)
    }

    @PreDestroy
    fun close() {
        clients.forEach { it.close() }
    }
}