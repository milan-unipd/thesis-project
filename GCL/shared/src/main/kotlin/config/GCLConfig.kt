package config

data class GCLConfig(
    val isObserver: Boolean,
    val nodeIP: String,
    val port: Int,
    val raft: RaftConfig,
    val loopWait: Int? = null,
    val leaseTTL: Int? = null,
    val globalLeaderLockTTL: Int? = null,
    val etcd: EtcdConfig? = null,
    val patroni: PatroniConf? = null
)
