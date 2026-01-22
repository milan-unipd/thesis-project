package config

import kotlinx.serialization.Serializable

@Serializable
data class EtcdEndpointConf(
    val host: String,
    val port: Int
)
