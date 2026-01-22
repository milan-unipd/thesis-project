package gcl

import config.ConfigHolder
import kotlin.getOrElse

internal suspend fun GCL.runAsGlobalLeader() {
    logger.info("Node ${ConfigHolder.config.etcd!!.id} is the global leader after acquiring the leader lock")
    val newMasterInfo = MasterClusterInfo(
        ConfigHolder.config.raft.id,
        ConfigHolder.config.nodeIP,
        7432
    )
    if (!updateGlobalMasterInfo(newMasterInfo)) return
    if (!updateLocalMasterInfo(newMasterInfo)) return

    patroniContainerManager.runPatroniWithConfig(newMasterInfo)

    val snapshotData = raftClient.getLastSavedSnapshotData()
    if (snapshotData != null)
        etcdClient.writeSnapshotData(snapshotData).getOrElse {
            logger.error("Failed to write snapshot data ${it.message}")
            if (!etcdClient.isLeader().getOrElse { false }) raftClient.stop()
            patroniContainerManager.stopPatroni()
            return
        }

}

private suspend fun GCL.updateGlobalMasterInfo(newMasterInfo: MasterClusterInfo): Boolean {
    val oldGlobalMasterInfo = raftClient.getMasterClusterInfo().getOrElse {
        logger.error("Failed to get global master info ${it.message}")
        if (!etcdClient.isLeader().getOrElse { false }) raftClient.stop()
        patroniContainerManager.stopPatroni()
        return false
    }
    if (oldGlobalMasterInfo != newMasterInfo) {
        raftClient.updateMasterClusterInfo(newMasterInfo).getOrElse {
            logger.error("Failed to update global master info ${it.message}")
            if (!etcdClient.isLeader().getOrElse { false }) raftClient.stop()
            patroniContainerManager.stopPatroni()
            return false
        }
    }
    return true
}


