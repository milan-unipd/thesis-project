package global.state_machine.commands

import gcl.MasterClusterInfo
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
sealed class MasterClusterInfoCommand : StateMachineCommand() {

    @Serializable
    @SerialName("masterInfo")
    data class UpdateMasterClusterInfo(val masterClusterInfo: MasterClusterInfo) : MasterClusterInfoCommand()

    @Serializable
    @SerialName("getMasterInfo")
    object GetMasterClusterInfo : MasterClusterInfoCommand()
}