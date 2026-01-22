package global.state_machine.commands

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
sealed class NoRRCommand : StateMachineCommand() {

    @Serializable
    @SerialName("getNoRR")
    object GetNoRR : NoRRCommand()

    @Serializable
    @SerialName("setNoRR")
    data class SetNoRR(
        val old: Int,
        val new: Int,
    ) : NoRRCommand()


}