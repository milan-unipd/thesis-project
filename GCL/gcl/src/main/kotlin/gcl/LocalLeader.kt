package gcl

import config.ConfigHolder
import local.awaitParticipants
import local.enterBarrier
import local.releaseBarrier
import org.apache.ratis.util.LifeCycle


internal suspend fun GCL.runAsLocalLeader() {
    logger.info("Node ${ConfigHolder.config.etcd!!.id} is the local leader")

    startRaftServer()
    if (!isRaftServerRunning()) return


    updateGlobalNoRR().getOrElse {
        logger.error("Failed to update global num_of_remote_replicas ${it.message}")
        if (!etcdClient.isLeader().getOrElse { false }) {
            raftClient.stop()
            patroniContainerManager.stopPatroni()
        }
        return
    }
    updateLocalNoRR().getOrElse {
        logger.error("Failed to update local num_of_remote_replicas ${it.message}")
        return
    }

    val leaderLockResponse = acquireLeaderLock() ?: return

    when (leaderLockResponse) {
        "ACQUIRED", "RENEWED" -> runAsGlobalLeader()
        "BUSY" -> runAsGlobalFollower()
    }
}


private suspend fun GCL.startRaftServer() {
    if (!raftClient.started) {
        val snapshot = etcdClient.readSnapshotData().getOrElse {
            logger.error("Failed to read snapshot data from etcd")
            raftClient.stop()
            return
        }
        raftClient.start(snapshot)
    }
}

private fun GCL.isRaftServerRunning(): Boolean {
    return when (val state = raftClient.state()) {
        LifeCycle.State.NEW,
        LifeCycle.State.STARTING -> {
            false
        }

        LifeCycle.State.RUNNING -> {
            raftClient.getLeader() != null
        }

        LifeCycle.State.PAUSING,
        LifeCycle.State.PAUSED,
        LifeCycle.State.EXCEPTION,
        LifeCycle.State.CLOSING,
        LifeCycle.State.CLOSED -> {
            logger.error(
                "Raft server in bad state: $state \n" +
                        "Stopping Patroni and RaftClient"
            )
            patroniContainerManager.stopPatroni()
            raftClient.stop()
            throw IllegalStateException("RaftClient in illegal state $state")
        }
    }
}

private suspend fun GCL.acquireLeaderLock(): String? {
    val leaderLockResponse = raftClient.acquireLeaderLock(globalLeaderTTL).getOrElse {
        logger.error("Failed to acquire/renew leader lock ${it.message}")
        if (!etcdClient.isLeader().getOrElse { false }) {
            raftClient.stop()
            patroniContainerManager.stopPatroni()
        }
        return null
    }
    return leaderLockResponse
}

internal suspend fun GCL.updateLocalMasterInfo(newMasterInfo: MasterClusterInfo): Boolean {
    val oldLocalMasterInfo = etcdClient.getMasterClusterInfo().getOrElse {
        logger.error("Failed to get local master info ${it.message}")
        if (!etcdClient.isLeader().getOrElse { false }) raftClient.stop()
        patroniContainerManager.stopPatroni()
        return false
    }
    if (oldLocalMasterInfo == newMasterInfo) return true

    runCatching {
        var barrier = etcdClient.getPatroniBarrierName().getOrElse {
            logger.error("Failed to get Patroni barrier name ${it.message}")
            throw it
        }

        if (barrier == null) {
            barrier = "patroni_config_barrier"
            etcdClient.putPatroniBarrierName(barrier).getOrElse {
                logger.error("Failed to put Patroni barrier name in etdc ${it.message}")
                throw it
            }
        }

        patroniContainerManager.stopPatroni()

        etcdClient.enterBarrier(barrier).getOrElse {
            logger.error("Failed to enter into Patroni barrier ${it.message}")
            throw it
        }

        etcdClient.awaitParticipants(barrier, etcdClient.groupSize / 2 + 1, loopWait / 2).getOrElse {
            logger.error("Error while waiting on participants ${it.message}")
            throw it
        }

        etcdClient.putMasterClusterInfo(newMasterInfo).getOrElse {
            logger.error("Failed to put new local master info ${it.message}")
            throw it
        }

        etcdClient.removePatroniBarrierName().getOrElse {
            logger.error("Failed to remove Patroni barrier name ${it.message}")
            throw it
        }

        etcdClient.releaseBarrier(barrier).getOrElse {
            logger.error("Failed to release barrier ${it.message}")
            throw it
        }
        return true
    }
    if (!etcdClient.isLeader().getOrElse { false }) raftClient.stop()
    patroniContainerManager.stopPatroni()
    return false
}


private suspend fun GCL.updateGlobalNoRR() = runCatching {
    val norrUpdate = etcdClient.getNORRUpdate().getOrThrow() ?: return@runCatching true
    val (expectedNoRR, newNoRR) = norrUpdate
    raftClient.updateNoRR(expectedNoRR, newNoRR).getOrThrow()
    etcdClient.setNoRR(newNoRR).getOrThrow()
    patroniContainerManager.updateNoRR(newNoRR).getOrThrow()
    etcdClient.removeNoRRUpdate().getOrThrow()
}

private suspend fun GCL.updateLocalNoRR() = runCatching {
    val localNoRR = etcdClient.getNoRR().getOrElse {
        raftClient.stop()
        patroniContainerManager.stopPatroni()
        throw it
    }

    val globalNoRR = raftClient.getNoRR().getOrElse {
        if (!etcdClient.isLeader().getOrElse { false }) {
            raftClient.stop()
            patroniContainerManager.stopPatroni()
        }
        throw it
    }

    if (localNoRR == null || localNoRR != globalNoRR) {

        etcdClient.setNoRR(globalNoRR).getOrElse {
            it.printStackTrace()

            raftClient.stop()
            patroniContainerManager.stopPatroni()
            throw it
        }

        patroniContainerManager.updateNoRR(globalNoRR).getOrThrow()

    }

}