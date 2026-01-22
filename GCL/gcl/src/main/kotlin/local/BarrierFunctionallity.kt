package local

import config.ConfigHolder
import io.etcd.jetcd.options.DeleteOption
import io.etcd.jetcd.options.GetOption
import io.etcd.jetcd.watch.WatchEvent
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlin.coroutines.resume


suspend fun EtcdClient.enterBarrier(barrierId: String) = runCatching {
    val key = "/gcl/$groupID/barriers/$barrierId/enter/${ConfigHolder.config.etcd!!.id}"
    client.kvClient.put(
        byteSequence(key),
        byteSequence("ready"),
    ).await()
}

suspend fun EtcdClient.awaitBarrierRelease(barrierId: String, ttlSeconds: Long) = runCatching {
    withContext(Dispatchers.IO) {
        val releaseKey = byteSequence("/gcl/$groupID/barriers/$barrierId/release")
        withTimeout(ttlSeconds * 1000) {
            suspendCancellableCoroutine { continuation ->
                val watch = client.watchClient.watch(releaseKey) { response ->
                    if (response.events.any { it.eventType == WatchEvent.EventType.PUT || it.eventType == WatchEvent.EventType.DELETE })
                        continuation.resume(Unit)
                }
                continuation.invokeOnCancellation {
                    watch.close()
                }
            }
        }
    }
}

suspend fun EtcdClient.awaitParticipants(
    barrierId: String,
    expected: Int,
    ttlSeconds: Long
) = runCatching {
    withContext(Dispatchers.IO) {

        val prefix = byteSequence("/gcl/$groupID/barriers/$barrierId/enter")
        val count = client.kvClient.get(prefix, GetOption.builder().isPrefix(true).build()).await().count
        if (count >= expected) return@withContext


        withTimeout(ttlSeconds * 1000) {
            while (isActive) {
                val count = client.kvClient.get(prefix, GetOption.builder().isPrefix(true).build()).await().count
                if (count >= expected) {
                    return@withTimeout
                }
                delay(100)
            }
        }
    }
}

suspend fun EtcdClient.releaseBarrier(barrierId: String) = runCatching {
    val releaseKey = byteSequence("/gcl/$groupID/barriers/$barrierId")
    client.kvClient.delete(releaseKey, DeleteOption.builder().isPrefix(true).build()).await()
}