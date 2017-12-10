package io.jgille.gcp.pubsub.bridge.publish

import spock.lang.Specification
import spock.lang.Unroll

class PathMatcherSpec extends Specification {

    @Unroll
    def "'#p1' matches '#p2'"(String p1, String p2) {
        expect:
        PathMatcher.pathMatch(p1, p2)

        where:
        p1         | p2
        "/foo"     | "/foo"
        "/foo/"    | "/foo"
        "/foo"     | "/foo/"
        "/foo/bar" | "/foo/bar"
    }

    @Unroll
    def "'#p1' does not '#p2'"(String p1, String p2) {
        expect:
        !PathMatcher.pathMatch(p1, p2)

        where:
        p1         | p2
        "/foo"     | "foo"
        "/foo/bar" | "/foo"
        "/fo"      | "/foo/"
    }
}
