package config

import kotlinx.serialization.Serializable

@Serializable
data class RaftPeerConf(
    val id: String,
    val host: String,
    val port: Int
)