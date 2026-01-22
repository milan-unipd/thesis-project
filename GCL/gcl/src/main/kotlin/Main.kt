import config.ConfigHolder
import gcl.GCL
import global.state_machine.commands.CommandCodec
import global.state_machine.commands.NoRRCommand
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.protocol.Message
import java.nio.file.Files
import java.util.Properties
import kotlin.io.path.Path
import kotlin.text.Charsets.UTF_8

fun main(vararg args: String): Unit = runBlocking {

    if (args.isEmpty()) {
        println("Please specify config file path!")
        return@runBlocking
    }

    val configPath = args[0]

    ConfigHolder.readConfigFromFile(configPath)
    val gcl = GCL()

    Runtime.getRuntime().addShutdownHook(Thread {
        runBlocking {
            gcl.shutdown()
        }
    })

    gcl.start()

    awaitCancellation()
}

