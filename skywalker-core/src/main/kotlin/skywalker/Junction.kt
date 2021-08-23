package skywalker

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import java.util.concurrent.TimeUnit

@ExperimentalTime
interface Junction<K, V> {
    suspend fun send(id: K, message: V, timeout: Duration);
    suspend fun recv(id: K, timeout: Duration): V;
}

@ExperimentalTime
class LocalJunction<K, V>: Junction<K, V> {
    private val channels: LoadingCache<K, Channel<Channel<V>>> = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build(CacheLoader.from { _ -> Channel(1024) })

    private fun getChans(id: K): Channel<Channel<V>> = channels.get(id)

    override suspend fun send(id: K, message: V, timeout: Duration): Unit = coroutineScope{
        withTimeout(timeout) {
            val chans = getChans(id)
            val chan = chans.receive();
            chan.trySend(message)
            var reciveResult = chans.tryReceive()
            while (reciveResult.isSuccess) {
                reciveResult.getOrNull()?.trySend(message)
                reciveResult = chans.tryReceive()
            }
        }
    }

    override suspend fun recv(id: K, timeout: Duration): V = coroutineScope {
        withTimeout(timeout) {
            val chans = getChans(id)
            val chan = Channel<V>(Channel.RENDEZVOUS)
            chans.send(chan)
            chan.receive()
        }
    }
}