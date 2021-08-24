package skywalker

import com.daveanthonythomas.moshipack.MoshiPack
import com.daveanthonythomas.moshipack.TypeReference
import com.squareup.moshi.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import okio.*
import org.cliffc.high_scale_lib.NonBlockingHashMap
import org.cliffc.high_scale_lib.NonBlockingHashMapLong
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.atomic.AtomicLong

import skywalker.kotlinx.coroutines.experimental.nio.aRead
import skywalker.kotlinx.coroutines.experimental.nio.aWrite
import java.lang.IllegalArgumentException
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.lang.Exception

enum class MethodCall {
    Send,
    Receive
}

// RPC spec:
// Clients sends MessagePack array of elements:
// 1. Protocol version, integer, currently 0.
// 2. Message type, integer
//    0 -- send
//    1 -- recv
// 3. Message ID, integer
// 4+. Arguments for the method called.
//    for send:
//      1. ID sending to
//      2. value being sent (anything)
//      3. timeout in milliseconds
//    for receive
//      1. ID sending to
//      2. timeout in milliseconds
//
// Servers send MessagePack array of elements in response:
// 1. Protocol version, integer, currently 0.
// 2. Message type, integer
//    2 -- send response
//    3 -- recv response
//    4 -- error response
// 3. Message ID, integer (same one from the client)
// 4+. Arguments for response based on method called
//    If success
//      If send response, no more args (only 2)
//      If recv response, the value received (anything)
//    If error:
//      0 -- the call timed out
//      1 -- the call was cancelled
//      255 -- internal error, followed by string description of error

val VERSION = 0

val SEND = 0
val RECV = 1
val SEND_RESP = 2
val RECV_RESP = 3
val ERR_RESP = 4

val TIMEOUT_ERROR = 0
val CANCELLED_ERROR = 1

sealed class Request<K, V>(val messageId: Long);
@ExperimentalTime
data class SendRequest<K, V>(val mid: Long, val id: K, val value: V, val timeout: Duration): Request<K, V>(mid)
@ExperimentalTime
data class RecvRequest<K, V>(val mid: Long, val id: K, val timeout: Duration): Request<K, V>(mid)

sealed class Response<V>(val messageId: Long)
data class SendResponse<V>(val mid: Long): Response<V>(mid)
data class RecvResponse<V>(val mid: Long, val value: V): Response<V>(mid)
data class ErrorResponse<V>(val mid: Long, val error: Int, val message: String?): Response<V>(mid)

class RequestAdapter<K, V>(val moshi: Moshi): JsonAdapter<Request<K, V>>() {
    @ExperimentalTime
    @FromJson
    override fun fromJson(reader: JsonReader): Request<K, V>? {
        reader.beginArray()
        val version = reader.nextInt()
        if (version != VERSION) throw Exception("invalid version $version")
        when (reader.nextInt()) {
            SEND -> {
                val messageId = reader.nextLong()
                val timeout = reader.nextLong()
                val idAdapter = moshi.adapter<K>(object: TypeReference<K>(){}.type)
                val id = idAdapter.fromJson(reader)
                val valueAdapter = moshi.adapter<V>(object: TypeReference<V>(){}.type)
                val value = valueAdapter.fromJson(reader)
                reader.endArray()
                return SendRequest(messageId, id!!, value!!, Duration.milliseconds(timeout))
            }
            RECV -> {
                val messageId = reader.nextLong()
                val timeout = reader.nextLong()
                val idAdapter = moshi.adapter<K>(object: TypeReference<K>(){}.type)
                val id = idAdapter.fromJson(reader)
                reader.endArray()
                return RecvRequest(messageId, id!!, Duration.milliseconds(timeout))
            }
            else -> throw IllegalArgumentException("invalid command type")
        }
    }

    @ExperimentalTime
    @ToJson
    override fun toJson(writer: JsonWriter, value: Request<K, V>?) {
        when (value) {
            is SendRequest<K, V> -> {
                writer.beginArray()
                writer.value(VERSION)
                writer.value(SEND)
                writer.value(value.messageId)
                writer.value(value.timeout.inWholeMilliseconds)
                val idAdapter = moshi.adapter<K>(object: TypeReference<K>(){}.type)
                idAdapter.toJson(writer, value.id)
                val valueAdapter = moshi.adapter<V>(object: TypeReference<V>(){}.type)
                valueAdapter.toJson(writer, value.value)
                writer.endArray()
            }
            is RecvRequest<K, V> -> {
                writer.beginArray()
                writer.value(VERSION)
                writer.value(RECV)
                writer.value(value.messageId)
                writer.value(value.timeout.inWholeMilliseconds)
                val idAdapter = moshi.adapter<K>(object: TypeReference<K>(){}.type)
                idAdapter.toJson(writer, value.id)
                writer.endArray()
            }
            else -> throw IllegalArgumentException("invalid request object")
        }
    }
}

class ResponseAdapter<V>(val moshi: Moshi): JsonAdapter<Response<V>>() {
    @FromJson
    override fun fromJson(reader: JsonReader): Response<V>? {
        reader.beginArray()
        val version = reader.nextInt()
        if (version != VERSION) throw IllegalArgumentException("invalid version $version")
        when (reader.nextInt()) {
            SEND_RESP -> {
                val messageId = reader.nextLong()
                reader.endArray()
                return SendResponse<V>(messageId)
            }
            RECV_RESP -> {
                val messageId = reader.nextLong()
                val valueAdapter = moshi.adapter<V>(object: TypeReference<V>(){}.type)
                val value = valueAdapter.fromJson(reader)
                reader.endArray()
                return RecvResponse(messageId, value!!)
            }
            ERR_RESP -> {
                val messageId = reader.nextLong()
                val errorCode = reader.nextInt()
                val errorMsg = when(errorCode) {
                    255 -> reader.nextString()
                    else -> null
                }
                reader.endArray()
                return ErrorResponse<V>(messageId, errorCode, errorMsg)
            }
            else -> throw IllegalArgumentException("invalid response code")
        }
    }

    @ToJson
    override fun toJson(writer: JsonWriter, value: Response<V>?) {
        when (value) {
            is SendResponse<V> -> {
                writer.beginArray()
                writer.value(VERSION)
                writer.value(SEND_RESP)
                writer.value(value.messageId)
                writer.endArray()
            }
            is RecvResponse<V> -> {
                writer.beginArray()
                writer.value(VERSION)
                writer.value(RECV_RESP)
                writer.value(value.messageId)
                val valueAdapter = moshi.adapter<V>(object: TypeReference<V>(){}.type)
                valueAdapter.toJson(writer, value.value)
                writer.endArray()
            }
            is ErrorResponse<V> -> {
                writer.beginArray()
                writer.value(VERSION)
                writer.value(ERR_RESP)
                writer.value(value.messageId)
                writer.value(value.error)
                if (value.error == 255) {
                    writer.value(value.message ?: "")
                }
                writer.endArray()
            }
        }
    }

}

class TimeoutException: Exception()
class CancelledException: Exception()
class GeneralException(msg: String): Exception(msg)

@ExperimentalTime
class RemoteJunction<K, V>(val socketChannel: AsynchronousSocketChannel): Junction<K, V> {
    private val methodCalls: NonBlockingHashMapLong<Channel<Result<Any?>>> = NonBlockingHashMapLong()
    private val moshi: Moshi = Moshi.Builder().build()
    private val moshiPack: MoshiPack = MoshiPack({
        this.add(RequestAdapter<K, V>(moshi))
        this.add(ResponseAdapter<V>(moshi))
    })
    private val messageIds: AtomicLong = AtomicLong()
    private val running: AtomicBoolean = AtomicBoolean(false)

    override suspend fun send(id: K, message: V, timeout: Duration): Unit = coroutineScope {
        val msgId = messageIds.incrementAndGet()
        val replyChan = Channel<Result<Any?>>()
        val methodCall = SendRequest(msgId, id, message, timeout)
        val encoded = moshiPack.packToByteArray(methodCall)
        methodCalls[msgId] = replyChan
        try {
            val len = encoded.size
            val buffer = ByteBuffer.allocate(len + 2)
            buffer.putShort(len.toShort())
            buffer.put(encoded)
            val start = System.currentTimeMillis()
            socketChannel.aWrite(buffer, timeout.inWholeMilliseconds - (System.currentTimeMillis() - start), TimeUnit.MILLISECONDS)
            val response = replyChan.receive()
            if (response.isSuccess()) {
                response.value()
            } else {
                throw response.exception()
            }
        } finally {
            methodCalls.remove(msgId)
        }
    }

    override suspend fun recv(id: K, timeout: Duration): V = coroutineScope {
        val msgId = messageIds.incrementAndGet()
        val replyChan = Channel<Result<Any?>>()
        val methodCall = RecvRequest<K, V>(msgId, id, timeout)
        val encoded = moshiPack.packToByteArray(methodCall)
        methodCalls[msgId] = replyChan
        try {
            val len = encoded.size
            val buffer = ByteBuffer.allocate(len + 2)
            buffer.putShort(len.toShort())
            buffer.put(encoded)
            val start = System.currentTimeMillis()
            socketChannel.aWrite(buffer, timeout.inWholeMilliseconds - (System.currentTimeMillis() - start), TimeUnit.MILLISECONDS)
            val response = replyChan.receive()
            if (response.isSuccess()) {
                response.value() as V
            } else {
                throw response.exception()
            }
        } finally {
            methodCalls.remove(msgId)
        }
    }

    fun close() {
        socketChannel.close()
    }

    suspend fun start(): Job = coroutineScope {
        launch {
            val lengthBuffer = ByteBuffer.allocate(2)
            if (running.compareAndSet(false, true)) {
                while (running.get()) {
                    val read = socketChannel.aRead(lengthBuffer, 1, TimeUnit.SECONDS)
                    if (read == 2) {
                        if (read <= 1.shl(14)) {
                            val buffer = ByteBuffer.allocate(read)
                            val bread = socketChannel.aRead(buffer, 30, TimeUnit.SECONDS)
                            if (bread != read) {
                                throw IllegalArgumentException("short read, read $bread, expected $read")
                            }
                            buffer.flip()
                            val decoded = moshiPack.unpack<Response<V>>(buffer.array())
                            val channel = methodCalls[decoded.messageId]
                            when (decoded) {
                                is SendResponse<V> -> channel?.send(Ok(Unit))
                                is RecvResponse<V> -> channel?.send(Ok(decoded.value))
                                is ErrorResponse -> when (decoded.error) {
                                    TIMEOUT_ERROR -> channel?.send(Err(TimeoutException()))
                                    CANCELLED_ERROR -> channel?.send(Err(CancelledException()))
                                    else -> channel?.send(Err(GeneralException(decoded.message ?: "")))
                                }
                            }
                        } else {
                            throw IllegalArgumentException("data frame too big: $read bytes")
                        }
                    }
                }
            }
        }
    }

    fun stop() {
        running.set(false)
    }
}