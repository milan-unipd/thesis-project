package gcl

internal suspend fun GCL.runAsGlobalFollower() {

    val globalMasterInfo = raftClient.getMasterClusterInfo().getOrElse {
        logger.error("Failed to get global master info ${it.message}")
        if (!etcdClient.isLeader().getOrElse { false }) raftClient.stop()
        patroniContainerManager.stopPatroni()
        return
    }

    if (!updateLocalMasterInfo(globalMasterInfo)) return

    patroniContainerManager.runPatroniWithConfig(globalMasterInfo)

}
