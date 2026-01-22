package global

import config.ConfigHolder
import gcl.MasterClusterInfo
import global.state_machine.commands.*
import global.state_machine.machine.GCLStateMachine
import global.state_machine.machine.StateMachineEventResponder
import global.state_machine.snapshot.SnapshotData
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import org.apache.ratis.RaftConfigKeys
import org.apache.ratis.client.RaftClient
import org.apache.ratis.conf.Parameters
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.grpc.GrpcFactory
import org.apache.ratis.protocol.*
import org.apache.ratis.retry.RetryPolicies
import org.apache.ratis.rpc.SupportedRpcType
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys
import org.apache.ratis.util.LifeCycle
import org.apache.ratis.util.TimeDuration
import java.io.File
import java.nio.file.Files
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.io.path.Path
import kotlin.text.Charsets.UTF_8


class GCLRaftClient(
    appScope: CoroutineScope,
    private val port: Int = ConfigHolder.config.port,
    groupIDString: String = ConfigHolder.config.raft.groupID,
    initialPeers: List<RaftPeer> = ConfigHolder.config.raft.peers.map {
        RaftPeer.newBuilder()
            .setId(it.id)
            .setAddress("${it.host}:${it.port}")
            .build()
    },
    rootStoragePath: String = ConfigHolder.config.raft.rootStorage
) : StateMachineEventResponder {


    private val scope = CoroutineScope(appScope.coroutineContext + SupervisorJob())


    private val groupID: RaftGroupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupIDString.toByteArray(UTF_8)))
    private val group = RaftGroup.valueOf(groupID, initialPeers)
    private val storageDir = File("$rootStoragePath/${ConfigHolder.config.raft.id}")

    private lateinit var client: RaftClient
    private lateinit var server: RaftServer
    private lateinit var stateMachine: GCLStateMachine
    var started = false
        private set

    private var tickJob: Job? = null

    fun start(lastSnapshot: SnapshotData?) {
        if (started)
            return
        started = true
        server = createRaftServer(lastSnapshot)
        server.start()
        client = createRaftClient()

    }


    fun shutdown() {
        stop()
        runCatching { scope.cancel() }
    }

    fun stop() {
        if (!started) return
        started = false
        runCatching { server.close() }
        runCatching { client.close() }
    }

    private fun createRaftClient(leaderId: ClientId? = null): RaftClient {
        val clientProperties = RaftProperties()
        val retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            1,
            TimeDuration.valueOf((ConfigHolder.config.loopWait ?: 10) / 2L, TimeUnit.SECONDS)
        )
        val clientRpc = GrpcFactory(Parameters()).newRaftClientRpc(
            //ClientId.valueOf(UUID.nameUUIDFromBytes(ConfigHolder.config.raft.id.toByteArray(UTF_8))),
            leaderId ?: ClientId.randomId(),
            clientProperties
        )


        val builder = RaftClient.newBuilder()
            .setProperties(clientProperties)
            .setRaftGroup(group)
            .setRetryPolicy(retryPolicy)
            .setClientRpc(clientRpc)

        if (leaderId != null)
            builder.setClientId(leaderId)

        return builder.build()

    }

    fun loadRaftProperties(file: String): RaftProperties {
        val p = Properties()
        Files.newInputStream(Path(file)).use { `in` ->
            p.load(`in`)
        }
        val rp = RaftProperties()
        for (key in p.stringPropertyNames()) {
            rp.set(key, p.getProperty(key))
        }
        return rp
    }

    private fun createRaftServer(lastSnapshot: SnapshotData?): RaftServer {

        stateMachine = GCLStateMachine(lastSnapshot, this)
        val properties = loadRaftProperties("raft.properties")

        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC)
        GrpcConfigKeys.Server.setPort(properties, port)

        val serverBuilder = RaftServer.newBuilder()

        if (!storageDir.exists())
            serverBuilder.setGroup(group)



        RaftServerConfigKeys.setStorageDir(properties, listOf(storageDir))
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true)
        //RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 500)
        RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, 2)
        RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false)
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true)

        serverBuilder
            .setServerId(RaftPeerId.valueOf(ConfigHolder.config.raft.id))
            .setProperties(properties)
            .setStateMachine(stateMachine)

        return serverBuilder.build()
    }

    suspend fun acquireLeaderLock(ticks: Long = 10): Result<String> {
        val cmd = LockCommand.Acquire(
            lockId = "leader-lock",
            ownerId = ConfigHolder.config.raft.id,
            leaseTicks = ticks
        )
        return sendStateMachineCommand(cmd)
    }


    private suspend fun sendStateMachineCommand(cmd: StateMachineCommand): Result<String> = runCatching {
        withContext(Dispatchers.IO) {
            val response = client.io().send(cmd.toMessage())
            response.message.content.toStringUtf8()
        }
    }

    suspend fun getMasterClusterInfo(): Result<MasterClusterInfo> = runCatching {
        withContext(Dispatchers.IO) {
            val leaderId = server.getDivision(groupID).info.leaderId ?: throw RuntimeException("Leader not found")
            val cmd = MasterClusterInfoCommand.GetMasterClusterInfo
            val response = client.io().sendReadOnly(cmd.toMessage(), leaderId)
            Json.decodeFromString(response.message.content.toStringUtf8())
        }
    }

    suspend fun updateMasterClusterInfo(masterClusterInfo: MasterClusterInfo): Result<Unit> = runCatching {
        withContext(Dispatchers.IO) {
            server.getDivision(groupID).info.leaderId ?: throw RuntimeException("Leader not found")
            val cmd = MasterClusterInfoCommand.UpdateMasterClusterInfo(masterClusterInfo)
            val response = client.io().send(cmd.toMessage())
            if (response.message.content.toStringUtf8() != "UPDATED")
                throw RuntimeException("Unexpected response: ${response.message.content.toStringUtf8()}")
        }
    }

    suspend fun getNoRR() = runCatching {
        withContext(Dispatchers.IO) {
            val leaderId = getLeader() ?: throw RuntimeException("Leader not found")
            val cmd = NoRRCommand.GetNoRR
            val response = client.io().sendReadOnly(cmd.toMessage(), leaderId)
            response.message.content.toStringUtf8().toInt()
        }
    }

    suspend fun updateNoRR(expectedValue: Int, newValue: Int) = runCatching {
        withContext(Dispatchers.IO) {
            val cmd = NoRRCommand.SetNoRR(expectedValue, newValue)
            val response = client.io().send(cmd.toMessage())
            val responseMessage = response.message.content.toStringUtf8().toString()
            if (responseMessage != "OK" && responseMessage != "FAIL")
                throw RuntimeException("Unexpected response: ${response.message.content.toStringUtf8()}")
            responseMessage
        }
    }

    fun state(): LifeCycle.State = server.lifeCycleState

    fun getLastSavedSnapshotData(): SnapshotData? {
        if (!started) return null
        return stateMachine.getLastSavedSnapshotData()
    }

    fun getLeader(): RaftPeerId? = server.getDivision(groupID).info.leaderId

    override fun onLeader() {
        scope.launch(Dispatchers.IO) {
            if (!stateMachine.isBootstrapped) {
                val initLeader = ConfigHolder.config.raft.peers.find { it.id == ConfigHolder.config.raft.initialLeader }
                val msg = ClusterBootstrap(
                    0L,
                    initLeader!!.id,
                    MasterClusterInfo(
                        initLeader.id,
                        initLeader.host,
                        7432 //TODO
                    )
                ).toMessage()
                var result = runCatching {
                    client.io().send(msg).message.content.toStringUtf8()
                }
                while (result.isFailure) {
                    delay(500)
                    result = runCatching {
                        client.io().send(msg).message.content.toStringUtf8()
                    }
                }
            }

            tickJob = launch {
                while (isActive) {
                    runCatching {
                        client.io().send(LockCommand.Tick(1).toMessage())
                    }.getOrNull()
                    delay(1000)
                }
            }

        }
    }

    override fun onNotLeader() {
        tickJob?.cancel()
    }

}
