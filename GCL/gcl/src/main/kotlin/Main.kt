import config.ConfigHolder
import gcl.GCL
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking

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

