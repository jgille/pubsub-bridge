package io.jgille.gcp.pubsub.bridge.subscribe

import java.lang.Exception

open class RetryableException: RuntimeException {
    constructor()
    constructor(cause: Exception): super(cause)
}

class ServiceUnavailableException : RetryableException()