package global.state_machine.commands

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
sealed class LockCommand : StateMachineCommand() {
    @Serializable
    @SerialName("acquire")
    data class Acquire(
        val lockId: String,
        val ownerId: String,
        val leaseTicks: Long
    ) : LockCommand()

    @Serializable
    @SerialName("release")
    data class Release(
        val lockId: String,
        val ownerId: String
    ) : LockCommand()

    @Serializable
    @SerialName("tick")
    data class Tick(
        val amount: Long = 1
    ) : LockCommand()
}