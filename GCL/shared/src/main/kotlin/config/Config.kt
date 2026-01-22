package config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class Config(
    @SerialName("is_observer")
    val isObserver: Boolean = false,
    @SerialName("loop_wait")
    val loopWait: Int? = 10,
    @SerialName("avg_failover_time")
    val avgFailoverTime: Int? = 10,
    @SerialName("node_ip")
    val nodeIP: String,
    val port: Int = 9001,
    val etcd: EtcdConfig? = null,
    val raft: RaftConfig,
    val patroni: PatroniConf? = null,
)