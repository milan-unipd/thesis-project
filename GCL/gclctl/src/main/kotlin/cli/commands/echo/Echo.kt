package cli.commands.echo

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple


class Echo : CliktCommand(help = "Echo text") {
    private val text by argument().multiple()

    override fun run() {
        echo(text.joinToString(" "))
    }
}