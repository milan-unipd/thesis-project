package gcl

import config.ConfigHolder
import global.GCLRaftClient
import kotlinx.coroutines.*
import local.EtcdClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import patroni.PatroniContainerManager


class GCL {
    internal val logger: Logger = LoggerFactory.getLogger(GCL::class.java)

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    internal val loopWait = ConfigHolder.config.loopWait!!.toLong()
    internal val globalLeaderTTL = ConfigHolder.config.globalLeaderLockTTL

    internal val raftClient = GCLRaftClient(scope)
    internal val etcdClient by lazy { EtcdClient(scope) }
    internal val patroniContainerManager by lazy { PatroniContainerManager() }


    fun start() = CoroutineScope(Dispatchers.Default).launch {
        if (ConfigHolder.config.isObserver) {
            raftClient.start(null)
        } else {
            while (isActive) {
                val isLeader = etcdClient.isLeader().getOrElse {
                    logger.error("Failed to check leadership in local cluster, skipping iteration")
                    patroniContainerManager.stopPatroni()
                    continue
                }
                if (isLeader)
                    runAsLocalLeader()
                else
                    runAsLocalFollower()
                delay(loopWait * 1000)
            }
        }
    }

    fun shutdown() {
        runCatching { scope.cancel() }
        raftClient.shutdown()
        if (!ConfigHolder.config.isObserver) {
            etcdClient.shutdown()
            patroniContainerManager.shutdown()
        }
    }
}

