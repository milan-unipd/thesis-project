package gcl

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
@SerialName("masterInfo")
data class MasterClusterInfo(
    val masterGCLNodeId: String,
    val masterDBAddress: String,
    val masterDBPort: Int,
)