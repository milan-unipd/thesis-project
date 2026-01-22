package config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class EtcdConfig(
    @SerialName("group_id")
    val groupID: String,
    val id: String,
    val endpoints: List<EtcdEndpointConf>
)
