package io.jgille.gcp.pubsub.bridge.admin

import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.ApiException
import io.grpc.Status
import spock.lang.Specification

class PubSubAdminClientSpec extends Specification {

    private PubSubAdminClient client
    private TopicAndSubscriptionCreator topicAndSubscriptionCreator

    def setup() {
        topicAndSubscriptionCreator = Mock(TopicAndSubscriptionCreator)
        client = new PubSubAdminClient(topicAndSubscriptionCreator)
    }

    def "A topic is created if it doesn't exist"() {
        given:
        def topic = "topic"

        when:
        client.createTopicIfNecessary(topic)

        then:
        1 * topicAndSubscriptionCreator.createTopic(topic)
    }

    def "No exception is thrown if the topic already exists"() {
        given:
        def topic = "topic"
        def topicAlreadyExists = new ApiException(new RuntimeException(),
                GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false)
        topicAndSubscriptionCreator.createTopic(topic) >> { throw topicAlreadyExists }

        when:
        client.createTopicIfNecessary(topic)

        then:
        noExceptionThrown()
    }

    def "API exceptions are re-thrown when trying to create a topic"() {
        given:
        def topic = "topic"
        def aborted = new ApiException(new RuntimeException(),
                GrpcStatusCode.of(Status.Code.ABORTED), false)
        topicAndSubscriptionCreator.createTopic(topic) >> { throw aborted }

        when:
        client.createTopicIfNecessary(topic)

        then:
        def e = thrown(ApiException)
        e == aborted
    }

    def "A subscription is created if it doesn't exist"() {
        given:
        def topic = "topic"
        def subscription = "sub"

        when:
        client.createSubscriptionIfNecessary(topic, subscription)

        then:
        1 * topicAndSubscriptionCreator.createSubscription(topic, subscription)
    }

    def "No exception is thrown if the subscription already exists"() {
        def topic = "topic"
        def subscription = "sub"
        def subscriptionAlreadyExists = new ApiException(new RuntimeException(),
                GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false)
        topicAndSubscriptionCreator.createSubscription(topic, subscription) >> { throw subscriptionAlreadyExists }

        when:
        client.createSubscriptionIfNecessary(topic, subscription)

        then:
        noExceptionThrown()
    }

    def "API exceptions are re-thrown when trying to create a subscription"() {
        def topic = "topic"
        def subscription = "sub"
        def aborted = new ApiException(new RuntimeException(),
                GrpcStatusCode.of(Status.Code.ABORTED), false)
        topicAndSubscriptionCreator.createSubscription(topic, subscription) >> { throw aborted }

        when:
        client.createSubscriptionIfNecessary(topic, subscription)

        then:
        def e = thrown(ApiException)
        e == aborted
    }
}
