import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import skywalker.RemoteJunction
import skywalker.Server
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import kotlin.time.ExperimentalTime

import skywalker.kotlinx.coroutines.experimental.nio.aConnect
import kotlin.time.Duration

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestServer {
    var server: Server<String, Map<String, Any>>? = null
    var port: Int = -1
    var junction: RemoteJunction<String, Map<String, Any>>? = null

    @BeforeAll
    fun setup() {
        val channel = AsynchronousServerSocketChannel.open()
        channel.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
        port = (channel.localAddress as InetSocketAddress).port
    }

    @BeforeEach
    fun setupConnection() = runBlocking {
        val channel = AsynchronousSocketChannel.open()
        channel.aConnect(InetSocketAddress(InetAddress.getLoopbackAddress(), port))
        junction = RemoteJunction(channel)
    }

    @Test
    fun testSendTimeout() = runBlocking {
        try {
            junction?.send("testSendTimeout", mapOf(Pair("foo", "bar")), Duration.seconds(1))
            assert(false) { "expected timeout" }
        } catch (e: TimeoutCancellationException) {
            // pass
        }
    }

    @Test
    fun testRecvTimeout() = runBlocking {
        try {
            junction?.recv("testRecvTimeout", Duration.seconds(1))
        } catch (e: TimeoutCancellationException) {
            // pass
        }
    }

    @Test
    fun testSendRecv() = runBlocking {
        launch {
            delay(1000)
            junction?.send("testSendRecv", mapOf(Pair("foo", "bar")), Duration.seconds(5))
        }
        val v = junction?.recv("testSendRecv", Duration.seconds(5))
        assert(v == mapOf(Pair("foo", "bar")))
    }
}