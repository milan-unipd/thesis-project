package global.state_machine.snapshot

import gcl.MasterClusterInfo
import global.state_machine.LockEntry
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class SnapshotData(
    val lastCommitedTerm: Long,
    val lastCommitedIndex: Long,
    val masterClusterInfo: MasterClusterInfo?,
    val locks: Map<String, LockEntry>,
    val logicalTime: Long,
    val isBootstrapped: Boolean,
    val norr: Int
) {

    companion object SnapshotCodec {
        val json = Json {
            encodeDefaults = true
            ignoreUnknownKeys = true
            explicitNulls = false
            prettyPrint = false
        }

        fun deserialize(bytes: ByteArray): SnapshotData =
            json.decodeFromString(bytes.decodeToString())
    }

    fun serialize(): ByteArray =
        json.encodeToString(this).toByteArray()
}

