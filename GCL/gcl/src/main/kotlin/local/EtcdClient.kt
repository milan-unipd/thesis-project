package local

import config.ConfigHolder
import gcl.MasterClusterInfo
import global.state_machine.snapshot.SnapshotData
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.Client
import io.etcd.jetcd.election.NoLeaderException
import io.etcd.jetcd.lease.LeaseKeepAliveResponse
import io.etcd.jetcd.options.PutOption
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import java.net.URI
import kotlin.text.Charsets.UTF_8

class EtcdClient(
    appScope: CoroutineScope,
    endpoints: List<URI> = ConfigHolder.config.etcd!!.endpoints.map { URI.create("${it.host}:${it.port}") },
    id: String = ConfigHolder.config.etcd!!.id
) {

    private val scope = CoroutineScope(appScope.coroutineContext + SupervisorJob())

    internal val groupID = ConfigHolder.config.etcd!!.groupID
    private val ttl: Long = ConfigHolder.config.leaseTTL!!.toLong()
    private val leaderKey = byteSequence("/gcl/$groupID/leader")
    private val leaderProposal = byteSequence(id)
    private var leaseID = -1L

    private val masterClusterInfoKey = byteSequence("/gcl/$groupID/master_cluster_info")
    private val globalSnapshotKey = byteSequence("/gcl/$groupID/global/snapshot")
    private val norrUpdateKey = byteSequence("/gcl/$groupID/norr/update")
    private val noRRKey = byteSequence("/gcl/$groupID/norr/value")

    val groupSize = endpoints.size

    internal lateinit var client: Client

    internal fun byteSequence(s: String) = ByteSequence.from(s.toByteArray(UTF_8))


    init {
        scope.launch(Dispatchers.IO) {

            client = Client.builder()
                .endpoints(endpoints)
                .retryMaxAttempts(3)
                .waitForReady(true)
                .build()



            loop@ while (isActive) {
                try {
                    try {
                        if (!isLeader().getOrThrow()) {
                            delay(100)
                            continue@loop
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (_: NoLeaderException) {

                    } catch (e: Exception) {
                        e.printStackTrace()
                    }

                    leaseID = client.leaseClient.grant(ttl).await().id
                    launch {
                        client.leaseClient.keepAlive(leaseID, object : StreamObserver<LeaseKeepAliveResponse> {
                            override fun onNext(value: LeaseKeepAliveResponse?) = Unit
                            override fun onError(t: Throwable?) = cancel()
                            override fun onCompleted() = cancel()
                        })
                    }

                    client.electionClient.campaign(leaderKey, leaseID, leaderProposal).await()

                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    e.printStackTrace()
                    delay(1000)
                }
            }
        }
    }

    fun shutdown() {
        runCatching { scope.cancel() }
        runCatching { client.close() }
    }

    suspend fun isLeader(): Result<Boolean> = runCatching {
        val response = client.electionClient.leader(leaderKey).await()
        client.leaseClient.keepAliveOnce(leaseID).await()
        response.kv.value.toString(UTF_8) == leaderProposal.toString(UTF_8)
    }

    suspend fun getMasterClusterInfo(): Result<MasterClusterInfo?> = runCatching {
        val response = client.kvClient.get(masterClusterInfoKey).await()
        if (response.kvs?.size == 0)
            null
        else
            Json.decodeFromString<MasterClusterInfo>(response.kvs[0].value.toString(UTF_8))
    }

    suspend fun putMasterClusterInfo(masterClusterInfo: MasterClusterInfo) = runCatching {
        val value = Json.encodeToString(MasterClusterInfo.serializer(), masterClusterInfo).toByteArray(UTF_8)
        client.kvClient.put(
            masterClusterInfoKey,
            ByteSequence.from(value),
            PutOption.builder().withLeaseId(leaseID).build()
        ).await()
    }

    suspend fun putPatroniBarrierName(name: String) = runCatching {
        if (getPatroniBarrierName().getOrThrow() == null)
            client.kvClient.put(byteSequence("patroni_barrier_name"), byteSequence(name)).await()
    }

    suspend fun removePatroniBarrierName() = runCatching {
        client.kvClient.delete(byteSequence("patroni_barrier_name")).await()
    }

    suspend fun getPatroniBarrierName() = runCatching {
        val response = client.kvClient.get(byteSequence("patroni_barrier_name")).await()
        if (response.kvs.isNotEmpty())
            return@runCatching response.kvs[0].value.toString(UTF_8)
        null
    }

    suspend fun writeSnapshotData(snapshotData: SnapshotData) = runCatching {
        val value = byteSequence(snapshotData.serialize().toString())
        client.kvClient.put(globalSnapshotKey, value).await()
    }

    suspend fun readSnapshotData() = runCatching {
        val data = client.kvClient.get(globalSnapshotKey).await()
        if (data.kvs.isNotEmpty())
            SnapshotData.deserialize(data.kvs[0].value.bytes)
        else
            null
    }


    suspend fun getNORRUpdate() = runCatching {
        val response = client.kvClient.get(norrUpdateKey).await()
        if (response.kvs.isNotEmpty()) {
            val string = response.kvs[0].value.bytes.decodeToString()
            val split = string.split(",")
            Pair(split[0].toInt(), split[1].toInt())
        } else
            null
    }

    suspend fun removeNoRRUpdate() = runCatching {
        client.kvClient.delete(norrUpdateKey).await()
    }

    suspend fun getNoRR() = runCatching {
        val response = client.kvClient.get(noRRKey).await()
        if (response.kvs.isNotEmpty())
            response.kvs[0].value.bytes.decodeToString().toInt()
        else
            null
    }

    suspend fun setNoRR(value: Int) = runCatching {
        client.kvClient.put(noRRKey, byteSequence(value.toString())).await()

    }

}