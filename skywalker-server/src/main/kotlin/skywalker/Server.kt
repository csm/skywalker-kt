package skywalker

import com.daveanthonythomas.moshipack.MoshiPack
import com.squareup.moshi.Moshi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.nio.channels.AsynchronousServerSocketChannel

import skywalker.LocalJunction
import skywalker.kotlinx.coroutines.experimental.nio.aAccept
import skywalker.kotlinx.coroutines.experimental.nio.aRead
import skywalker.kotlinx.coroutines.experimental.nio.aWrite
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.ExperimentalTime

@ExperimentalTime
class Server<K, V>(private val channel: AsynchronousServerSocketChannel) {
    private val junction: LocalJunction<K, V> = LocalJunction()
    private val running = AtomicBoolean(false)
    private val channels: MutableSet<AsynchronousSocketChannel> = HashSet()
    private val moshi = Moshi.Builder().build()
    private val moshiPack = MoshiPack({
        add(RequestAdapter<K, V>(moshi))
        add(ResponseAdapter<V>(moshi))
    })

    suspend fun start() = coroutineScope {
        if (running.compareAndSet(false, true)) {
            launch {
                while (running.get()) {
                    val chan = channel.aAccept()
                    channels.add(chan)
                    handler(chan)
                }
            }
        }
    }

    suspend fun handler(channel: AsynchronousSocketChannel) = coroutineScope {
        launch {
            val lengthBuffer = ByteBuffer.allocate(2)
            while (channel.isOpen) {
                val read = channel.aRead(lengthBuffer, 30, TimeUnit.SECONDS)
                if (read == 2) {
                    val len = lengthBuffer.getShort(0).toInt()
                    if (len > 0 && len <= 1.shl(14)) {
                        val buffer = ByteBuffer.allocate(len)
                        val bread = channel.aRead(buffer, 30, TimeUnit.SECONDS)
                        if (bread == len) {
                            buffer.flip()
                            val request = moshiPack.unpack<Request<K, V>>(buffer.array())
                            launch {
                                val response = try {
                                    when (request) {
                                        is SendRequest<K, V> -> {
                                            junction.send(request.id, request.value, request.timeout)
                                            SendResponse(request.messageId)
                                        }
                                        is RecvRequest<K, V> -> {
                                            val value = junction.recv(request.id, request.timeout)
                                            RecvResponse(request.messageId, value)
                                        }
                                    }
                                } catch (timeout: TimeoutException) {
                                    ErrorResponse(request.messageId, TIMEOUT_ERROR, null)
                                } catch (e: Exception) {
                                    ErrorResponse(request.messageId, 255, e.message)
                                }
                                val encoded = moshiPack.packToByteArray(response)
                                val rbuffer = ByteBuffer.allocate(encoded.size + 2)
                                rbuffer.putShort(encoded.size.toShort())
                                rbuffer.put(encoded)
                                channel.aWrite(rbuffer, 30, TimeUnit.SECONDS)
                            }
                        } else {
                            println("error in request, read $bread, expected $len")
                        }
                    } else {
                        println("error in request, invalid length $len")
                        channel.close()
                        break
                    }
                }
            }
        }
    }

    fun stop() = running.set(false)
}