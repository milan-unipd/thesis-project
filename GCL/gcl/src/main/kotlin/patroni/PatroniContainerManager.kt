package patroni

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.StreamType
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.okhttp.OkDockerHttpClient
import config.ConfigHolder
import gcl.MasterClusterInfo
import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.TimeUnit
import kotlin.io.path.writeText
import kotlin.system.exitProcess
import kotlin.text.Charsets.UTF_8

class PatroniContainerManager(
    patroniConfigFilePath: String = ConfigHolder.config.patroni!!.confFile,
    private val patroniContainerName: String = ConfigHolder.config.patroni!!.containerName
) {
    private val logger: Logger = LoggerFactory.getLogger(PatroniContainerManager::class.java)

    private val yaml = Yaml()
    val options = DumperOptions().apply {
        defaultFlowStyle = DumperOptions.FlowStyle.BLOCK
        indent = 2
        isPrettyFlow = true
    }
    private val patroniConfigPath = Path.of(patroniConfigFilePath)
    private val patroniConfigFile: File = patroniConfigPath.toFile()

    private val dockerConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    private val httpClient = OkDockerHttpClient.Builder()
        .dockerHost(dockerConfig.dockerHost)
        .connectTimeout(30000)
        .readTimeout(45000)
        .build()

    private val dockerClient: DockerClient = DockerClientBuilder.getInstance().withDockerHttpClient(httpClient).build()

    private fun readPatroniConfigsFromFile(): MutableMap<String, Any> {
        val map = yaml.load<Map<String, Any>>(
            patroniConfigFile.readText(UTF_8)
        ) ?: throw RuntimeException("Failed to read Patroni config file")
        return map.toMutableMap()
    }


    suspend fun setPatroniToMasterCluster() {
        val root = readPatroniConfigsFromFile()
        val bootstrap = (root["bootstrap"] as? Map<*, *>)?.toMutableMap()
            ?: return   // nothing to do

        val dcs = (bootstrap["dcs"] as? Map<*, *>)?.toMutableMap()
            ?: return   // nothing to do

        if (dcs.containsKey("standby_cluster")) {
            dcs.remove("standby_cluster")
            // reattach modified maps
            bootstrap["dcs"] = dcs
            root["bootstrap"] = bootstrap
        }

        if (saveConfigAndRestart(root).isSuccess) {

            var success = false
            val callback = object : ResultCallback.Adapter<Frame>() {
                override fun onNext(frame: Frame) {
                    when (frame.streamType) {
                        StreamType.STDOUT -> {
                            success = frame.payload.toString(UTF_8).contains("successfully promoted")
                        }

                        StreamType.STDERR -> {
                            success =
                                frame.payload.toString(UTF_8).contains("Cluster is already in the required state")
                        }

                        StreamType.STDIN -> Unit
                        StreamType.RAW -> Unit
                    }
                }
            }
            var tries = 0
            while (!success && tries++ < 10) {
                val cmd = dockerClient.execCreateCmd(patroniContainerName)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withCmd("patronictl", "promote-cluster", "--force")
                    .exec()
                dockerClient.execStartCmd(cmd.id).exec(callback).awaitCompletion()
                delay(3000)
            }

            if (!success && tries == 10) {
                logger.error("FAILED TO PROMOTE PATRONI CLUSTER")
                exitProcess(-1)
            }
        }
    }

    private fun saveConfigAndRestart(root: Any) = runCatching {
        savePatroniConfig(root)
        restartPatroni()
    }

    private fun savePatroniConfig(root: Any) {
        val output = Yaml(options).dump(root)

        val tmp = Files.createTempFile(patroniConfigPath.parent, "patroni", ".tmp")
        tmp.writeText(output)
        Files.move(
            tmp,
            patroniConfigPath,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE,
        )

        Files.setPosixFilePermissions(
            patroniConfigPath,
            PosixFilePermissions.fromString("rw-r--r--")
        )
    }

    suspend fun setPatroniToStandbyCluster(newMasterInfo: MasterClusterInfo) {
        val root =
            (yaml.load<Map<String, Any>>(patroniConfigFile.readText(UTF_8)) ?: mutableMapOf()).toMutableMap()

        val bootstrap = (root["bootstrap"] as? Map<*, *>)?.toMutableMap()
            ?: mutableMapOf()

        val dcs = (bootstrap["dcs"] as? Map<*, *>)?.toMutableMap()
            ?: mutableMapOf()

        if (dcs.containsKey("standby_cluster")) {
            runCatching {
                val curStandbyClusterConf = dcs["standby_cluster"] as? Map<*, *>
                val curHost = curStandbyClusterConf?.get("host")
                val curPort = curStandbyClusterConf?.get("port")
                if (curHost == newMasterInfo.masterDBAddress && curPort == newMasterInfo.masterDBPort) {
                    if (isPatroniRunning().getOrNull() == false)
                        startPatroni()
                    return
                }
            }
        }

        dcs["standby_cluster"] = mapOf(
            "host" to newMasterInfo.masterDBAddress,
            "port" to newMasterInfo.masterDBPort,
            "create_replica_methods" to listOf("basebackup")
        )

        bootstrap["dcs"] = dcs
        root["bootstrap"] = bootstrap

        if (saveConfigAndRestart(root).isSuccess) {


            var success = false
            val callback = object : ResultCallback.Adapter<Frame>() {
                override fun onNext(frame: Frame) {
                    when (frame.streamType) {
                        StreamType.STDOUT -> {
                            success = frame.payload.toString(UTF_8).contains("successfully")
                        }

                        StreamType.STDERR -> {
                            success =
                                frame.payload.toString(UTF_8).contains("Cluster is already in the required state")
                        }

                        StreamType.STDIN -> Unit
                        StreamType.RAW -> Unit
                    }
                }
            }
            var tries = 0
            while (!success && tries++ < 10) {
                val cmd = dockerClient.execCreateCmd(patroniContainerName)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withCmd(
                        "patronictl", "demote-cluster",
                        "--host", newMasterInfo.masterDBAddress,
                        "--port", "7432",
                        "--force"
                    )
                    .exec()
                dockerClient.execStartCmd(cmd.id).exec(callback).awaitCompletion()
                delay(3000)
            }

            if (!success && tries == 10) {
                logger.error("FAILED TO DEMOTE PATRONI CLUSTER")
                exitProcess(-1)
            }
        }
    }


    fun shutdown() = runCatching {
        dockerClient.close()
    }

    fun stopPatroni() = runCatching {
        dockerClient.stopContainerCmd(patroniContainerName).exec()
    }

    private fun restartPatroni() = runCatching {

        if (isPatroniRunning().getOrThrow())
            dockerClient.restartContainerCmd(patroniContainerName).exec()
        else
            startPatroni()
    }

    fun startPatroni() = runCatching {
        dockerClient.startContainerCmd(patroniContainerName).exec()
    }

    fun isPatroniRunning() = runCatching {
        dockerClient.inspectContainerCmd(patroniContainerName).exec().state.running ?: false
    }

    suspend fun runPatroniWithConfig(masterClusterInfo: MasterClusterInfo) {
        val shouldBeMaster = masterClusterInfo.masterDBAddress == ConfigHolder.config.nodeIP
        val fromFileMasterInfo = getMasterInfoFromConfFile()
        if ((fromFileMasterInfo == null && shouldBeMaster) || masterClusterInfo == fromFileMasterInfo) {
            if (!isPatroniRunning().getOrElse { false })
                startPatroni()
            return
        }
        if (shouldBeMaster)
            setPatroniToMasterCluster()
        else
            setPatroniToStandbyCluster(masterClusterInfo)
    }

    fun getMasterInfoFromConfFile(): MasterClusterInfo? {

        val root = readPatroniConfigsFromFile()
        val bootstrap = (root["bootstrap"] as? Map<*, *>)?.toMutableMap() ?: mutableMapOf()
        val dcs = (bootstrap["dcs"] as? Map<*, *>)?.toMutableMap() ?: mutableMapOf()
        return if (!dcs.containsKey("standby_cluster")) {
            null
        } else {
            val standbyClusterConf = (dcs["standby_cluster"] as? Map<*, *>)!!.toMutableMap()
            val masterDBAddress = standbyClusterConf["host"] as String
            val port = standbyClusterConf["port"] as Int
            MasterClusterInfo("", masterDBAddress, port)
        }
    }

    fun updateNoRR(value: Int) = runCatching {
        updateNoRRInContainer(value).getOrThrow() && updateNoRRInConfigFile(value).getOrThrow()
    }

    private fun updateNoRRInConfigFile(value: Int) = runCatching {
        val root = readPatroniConfigsFromFile()
        val bootstrap = (root["bootstrap"] as? Map<*, *>)?.toMutableMap() ?: mutableMapOf()
        val dcs = (bootstrap["dcs"] as? Map<*, *>)?.toMutableMap() ?: mutableMapOf()
        dcs["num_of_remote_sync_replicas"] = value.toString()
        bootstrap["dcs"] = dcs
        root["bootstrap"] = bootstrap
        savePatroniConfig(root)
        true
    }

    private fun updateNoRRInContainer(value: Int) = runCatching {
        if (!isPatroniRunning().getOrElse { false })
            return@runCatching true
        var success = false
        val callback = object : ResultCallback.Adapter<Frame>() {
            override fun onNext(frame: Frame) {
                when (frame.streamType) {
                    StreamType.STDOUT -> {
                        success = frame.payload.toString(UTF_8).contains("set to $value")
                    }

                    StreamType.STDERR -> {
                        success =
                            !frame.payload.toString(UTF_8).contains("Failed")
                    }

                    StreamType.STDIN -> Unit
                    StreamType.RAW -> Unit
                }
            }
        }
        val cmd = dockerClient.execCreateCmd(patroniContainerName)
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd("patronictl", "set-remote-sync-replicas", "--force", "$value")
            .exec()
        dockerClient.execStartCmd(cmd.id).exec(callback).awaitCompletion(10, TimeUnit.SECONDS)
        success
    }


}