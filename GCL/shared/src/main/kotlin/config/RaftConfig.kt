package config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class RaftConfig(
    @SerialName("group_id")
    val groupID: String,
    val id: String,
    val peers: List<RaftPeerConf>,
    @SerialName("root_storage")
    val rootStorage: String,
    @SerialName("initial_leader")
    val initialLeader: String
)