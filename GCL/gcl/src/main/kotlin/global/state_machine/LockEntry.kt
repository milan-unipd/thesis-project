package global.state_machine

import kotlinx.serialization.Serializable

@Serializable
data class LockEntry(
    val ownerId: String,
    var expiresAtTick: Long
)