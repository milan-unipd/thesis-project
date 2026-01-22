package global.state_machine.commands

import gcl.MasterClusterInfo
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
@SerialName("bootstrap")
data class ClusterBootstrap(
    val initialLogicalTime: Long,
    val initialLeaderLockOwner: String,
    val masterInfo: MasterClusterInfo
) : StateMachineCommand()
