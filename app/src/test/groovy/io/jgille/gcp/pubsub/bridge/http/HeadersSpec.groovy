package io.jgille.gcp.pubsub.bridge.http

import spock.lang.Specification

class HeadersSpec extends Specification {

    def "Correlation-Id and correlation_id are the same things.."() {
        when:
        def headers = Headers.fromMessageAttributes([
                correlation_id  : "cid1",
                "Correlation-Id": "cid2",
                "Correlation_ID": "cid3"])

        then:
        headers == [
                new HttpHeader("Correlation-Id", "cid1"),
                new HttpHeader("Correlation-Id", "cid2"),
                new HttpHeader("Correlation-Id", "cid3")
        ]
    }

    def "Attribute names are prefixed with x-message-attribute"() {
        when:
        def headers = Headers.fromMessageAttributes(["foo": "bar"])

        then:
        headers == [new HttpHeader("x-message-attribute-foo", "bar")]
    }

    def "Headers prefixed with x-message-attribute- are included as message attributes"() {
        when:
        def attributes = Headers.toMessageAttributes([
                new HttpHeader("a", "b"),
                new HttpHeader("x-message-attribute-a", "foo"),
                new HttpHeader("X-Message-Attribute-Bar", "bar")
        ])

        then:
        attributes.size() == 2

        and:
        attributes.a == "foo"
        attributes.Bar == "bar"
    }
}
