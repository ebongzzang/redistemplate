package org.ebong2.featuretest.redistemplate

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest
import org.springframework.data.redis.core.HashOperations
import org.springframework.data.redis.core.RedisTemplate
import redis.embedded.RedisServer
import spock.lang.Specification

@DataRedisTest
class RedisTemplateUnitTest extends Specification {

    private static Logger log = LoggerFactory.getLogger(RedisTemplateUnitTest.class)

    @Autowired
    RedisTemplate<Object, Object> redisTemplate
    
    RedisServer redisServer = new RedisServer()

    def setup() {
        redisServer.start()
    }

    def cleanup() {
        redisServer.stop()
    }

    def "Test Hash type CRUD operations"() {

        given:
        HashOperations<String, String, Long> redisOperation = redisTemplate.opsForHash()
        def redisHashKey = "TestHashKey_1"
        def hashData = [imkey: 1]

        when:
        redisOperation.putAll(redisHashKey, hashData)

        then:
        redisOperation.get(redisHashKey, "imkey") == 1

        when: "insert new key and delete recently added value"
        redisOperation.put(redisHashKey, "imalsokey", 2)
        redisOperation.delete(redisHashKey, "imalsokey")

        then: "imalsokey should be deleted"
        redisOperation.get(redisHashKey, "imalsokey") == null
        redisOperation.entries(redisHashKey).size() == 1

        when: "update first insert key"
        redisOperation.put(redisHashKey, "imkey", 11)

        then: "it should be update existing value"
        redisOperation.get(redisHashKey, "imkey") == 11
    }

    def "Test SortedSet type CRUD operations"() {

        given:
        def redisSortedSetKey = "Test_sortedSetKey_1"
        def redisOperation = redisTemplate.boundZSetOps(redisSortedSetKey)

        when:
        redisOperation.add("imkey1", 1)
        redisOperation.add("imkey3", 3)
        redisOperation.add("imkey4", 4)
        redisOperation.add("imkey5", 5)
        redisOperation.add("imkey2", 2)
        redisOperation.add("imkey6", 6)
        redisOperation.add("imkey7", 7)

        then: "ordered by score"
        redisOperation.rank("imkey4") == 3
        redisOperation.rank("imkey2") == 1
        redisOperation.range(0, redisOperation.zCard() - 1).toArray()[2] == "imkey3"

        when: "select score place between 1 and 5"
        def sortedByScore = redisOperation.rangeByScore(1, 5)

        then: "result should be sorted by score regardless insert order"
        sortedByScore.toArray()[1] == "imkey2"
        sortedByScore.size() == 5

        when: "update imkey2's score as 10"
        redisOperation.add("imkey2", 10)

        then: "imkey2's should be place at end "
        redisOperation.rank("imkey2") == 6

        when:
        def sortedByScore2 = redisOperation.rangeByScore(1, 10)

        then:
        sortedByScore2.last() == "imkey2"

        when:
        def hi = redisOperation.rangeByScore(0, 1)
        def hi2 = redisOperation.reverseRangeByScore(10, 1)

        then:
        log.debug(hi.toString())
        log.debug(hi2.toString())

    }

    def "Test when sorted set containing same score values"() {
        given:
        def redisSortedSetKey = "Test_sortedSetKey_1"
        def redisOperation = redisTemplate.boundZSetOps(redisSortedSetKey)

        when: "values should be guaranteed unique, but score can duplicated"
        redisOperation.add("a", 1)
        redisOperation.add("ab", 1)
        redisOperation.add("abc", 1)
        redisOperation.add("abcd", 1)
        redisOperation.add("abcde", 1)

        then: "when value's score duplicated, it ordered by insertion order"
        redisOperation.rank("abc") == 2

    }

}