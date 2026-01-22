package cli.commands.num_of_remote_replicas

import cli.util.Spinner
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import config.ConfigHolder
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.Client
import io.etcd.jetcd.op.Cmp
import io.etcd.jetcd.op.CmpTarget
import io.etcd.jetcd.op.Op
import io.etcd.jetcd.options.PutOption
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import java.net.URI
import kotlin.system.exitProcess
import kotlin.text.Charsets.UTF_8

sealed class NoRRCommand(
    name: String,
    help: String
) : CliktCommand(name = name, help = help) {


    object GetNoRR : NoRRCommand(
        name = "get",
        help = "Get number of remote replicas"
    ) {

        override fun run(): Unit = runBlocking {
            runCatching {
                val client = createEtcdClient()
                val groupID = ConfigHolder.config.etcd!!.groupID
                val norrKey = ByteSequence.from("/gcl/$groupID/norr/value".toByteArray(UTF_8))
                val norr = client.kvClient.get(norrKey).await().kvs[0].value
                println("Number of remote replicas = $norr")
            }.getOrElse {
                println("An error occurred: ${it.message}")
            }
        }
    }

    object SetNoRR : NoRRCommand(
        name = "set",
        help = "Set's the number of remote replicas"
    ) {
        private val norr by argument(help = "Number of remote replicas")

        override fun run(): Unit = runBlocking {

            val spinner = Spinner()
            runCatching {
                if (ConfigHolder.config.isObserver) {
                    println("Can't update from observer nodes!")
                    exitProcess(0)
                }

                val client = createEtcdClient()



                spinner.start(this)
                val groupID = ConfigHolder.config.etcd!!.groupID
                val updateKey = ByteSequence.from("/gcl/$groupID/norr/update".toByteArray(UTF_8))
                val norrKey = ByteSequence.from("/gcl/$groupID/norr/value".toByteArray(UTF_8))
                val prev = client.kvClient.get(norrKey).await().kvs.firstOrNull()?.value?.toString(UTF_8) ?: "0"

                val txn = client.kvClient.txn()
                    .If(
                        Cmp(
                            updateKey, Cmp.Op.EQUAL, CmpTarget.version(0)
                        )
                    ).Then(
                        Op.put(updateKey, ByteSequence.from("$prev,$norr".toByteArray(UTF_8)), PutOption.DEFAULT)
                    ).Else()
                    .commit()

                if (!txn.await().isSucceeded) {
                    throw Exception("Someone else is updating the value or something went wrong")
                }


                while (isActive) {
                    val response = client.kvClient.get(updateKey).await() ?: break
                    if (response.kvs.isEmpty()) break
                    delay(100)
                }
                spinner.stop()

                val response = client.kvClient.get(norrKey).await()
                if (response.kvs[0].value.toString(UTF_8) != norr) {
                    println("UPDATE FAILED: Someone else is updating the value")
                } else {
                    println("UPDATE SUCCESSFUL")
                }

                client.close()
            }.getOrElse {
                spinner.stop()
                println("ERROR: ${it.message}")
            }
        }
    }

    internal fun createEtcdClient(): Client {
        val endpoints = ConfigHolder.config.etcd!!.endpoints.map { URI.create("${it.host}:${it.port}") }
        val client = Client.builder()
            .endpoints(endpoints)
            .retryMaxAttempts(1)
            .waitForReady(true)
            .build()

        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                client?.close()
            }
        })
        return client
    }

}