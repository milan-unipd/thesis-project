package gcl

import config.ConfigHolder
import local.awaitBarrierRelease
import local.enterBarrier

internal suspend fun GCL.runAsLocalFollower() {
    logger.info("Node ${ConfigHolder.config.etcd!!.id} is a local follower")

    val barrier = etcdClient.getPatroniBarrierName().getOrElse {
        logger.error("Failed to get Patroni barrier name ${it.message}")
        patroniContainerManager.stopPatroni()
        return
    }

    if (barrier == null) {
        val masterClusterInfo = etcdClient.getMasterClusterInfo().getOrElse {
            logger.error("Failed to get local master info ${it.message}")
            patroniContainerManager.stopPatroni()
            return
        }

        if (masterClusterInfo == null) {
            patroniContainerManager.stopPatroni()
            return
        }

        patroniContainerManager.runPatroniWithConfig(masterClusterInfo)
        return
    }

    patroniContainerManager.stopPatroni()
    etcdClient.enterBarrier(barrier).getOrElse {
        logger.error("Failed to enter into Patroni barrier ${it.message}")
        return
    }
    etcdClient.awaitBarrierRelease(barrier, loopWait / 2).getOrElse {
        logger.error("Failed while waiting for barrier release: ${it.message}")
        return
    }

    val masterClusterInfo = etcdClient.getMasterClusterInfo().getOrElse {
        logger.error("Failed to get local master info ${it.message}")
        patroniContainerManager.stopPatroni()
        return
    } ?: return
    patroniContainerManager.runPatroniWithConfig(masterClusterInfo)

}