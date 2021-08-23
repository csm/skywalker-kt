package skywalker

import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class TestLocalJunction {
    @Test
    fun testSendTimeout() = runBlocking {
        val junct = LocalJunction<String, Boolean>()
        try {
            junct.send("test", true, Duration.seconds(1))
            assert(false) { "expected timeout" }
        } catch (e: TimeoutCancellationException) {
            // pass
        }
    }

    @Test
    fun testRecvTimeout() = runBlocking {
        val junct = LocalJunction<String, Boolean>()
        try {
            val result = junct.recv("test", Duration.Companion.seconds(1))
            assert(!result) { "expected timeout" }
        } catch (e: TimeoutCancellationException) {
            // pass
        }
    }

    @Test
    fun testSendRecv() = runBlocking {
        val junct = LocalJunction<String, String>()
        launch {
            delay(1000)
            try {
                junct.send("test", "foo", Duration.seconds(1))
            } catch (e: TimeoutCancellationException) {
                assert(false)
            }
        }
        val result = junct.recv("test", Duration.seconds(60))
        assert(result == "foo")
    }

    @Test
    fun testRecvSend() = runBlocking {
        val junct = LocalJunction<String, String>()
        launch {
            val result1 = junct.recv("test", Duration.seconds(5))
            assert(result1 == "foo")
        }
        launch {
            val result2 = junct.recv("test", Duration.seconds(5))
            assert(result2 == "foo")
        }
        delay(500)
        junct.send("test", "foo", Duration.seconds(5))
    }

    @Test
    fun testMany() = runBlocking {
        val items = Vector<String>(10_000)
        val junct = LocalJunction<String, String>()
        val jobs = Vector<Job>(10_000)
        repeat(10_000) { i ->
            launch {
                junct.send("test$i", "value$i", Duration.seconds(5))
            }
        }
        repeat(10_000) { i ->
            jobs.add(launch {
                val v = junct.recv("test$i", Duration.seconds(5))
                items.add(v)
            })
        }
        jobs.joinAll()
        assert(items.count() == 10_000)
    }
}