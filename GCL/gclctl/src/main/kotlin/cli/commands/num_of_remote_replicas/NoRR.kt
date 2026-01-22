package cli.commands.num_of_remote_replicas

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import config.ConfigHolder

class NoRR : CliktCommand(
    name = "number_of_remote_replicas",
    help = "Number of remote replicas operations"
) {
    private val file by option("-c", help = "Path to the config file gcl uses")


    override fun run() {
        ConfigHolder.readConfigFromFile(file ?: "gcl_config.yml")
    }

}