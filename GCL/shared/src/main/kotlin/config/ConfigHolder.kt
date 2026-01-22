package config

import com.charleskorn.kaml.Yaml
import java.io.File

object ConfigHolder {

    private var _config: GCLConfig? = null

    val config: GCLConfig
        get() = _config ?: error("Config not initialized")

    fun init(config: GCLConfig) {
        check(_config == null) { "Config already initialized" }
        _config = config
    }

    fun readConfigFromFile(configFilePath: String = "gcl_config.yml") {
        val yaml = Yaml.default
        val config = yaml.decodeFromString(
            Config.serializer(),
            File(configFilePath).readText()
        )

        if (config.isObserver) {
            val gclConfig = GCLConfig(
                isObserver = true,
                nodeIP = config.nodeIP,
                port = config.port,
                raft = config.raft
            )

            init(gclConfig)
            return
        }

        require(config.loopWait!! > 0) { "Loop wait must be positive" }
        require(config.avgFailoverTime!! > 0) { "Failover time must be positive" }
        require(config.nodeIP.isNotBlank()) { "Node IP must not be blank" }
        require(config.port in 1024..65535) { "Port must be between 1024 and 65535" }

        var barrierTTL = 2 * config.loopWait
        var leaseTTL = 0
        while (leaseTTL <= 0) {
            leaseTTL = (barrierTTL / 1.5 - config.avgFailoverTime).toInt()
            if (leaseTTL < 1) {
                barrierTTL += 1
            }
        }

        val gclConfig = GCLConfig(
            isObserver = false,
            loopWait = config.loopWait,
            leaseTTL = leaseTTL,
            globalLeaderLockTTL = barrierTTL,
            nodeIP = config.nodeIP,
            port = config.port,
            etcd = config.etcd!!,
            raft = config.raft,
            patroni = config.patroni!!
        )

        init(gclConfig)
    }
}