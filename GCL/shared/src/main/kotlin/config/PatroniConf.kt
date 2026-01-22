package config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class PatroniConf(
    @SerialName("conf_file")
    val confFile: String,
    @SerialName("container_name")
    val containerName: String
)
