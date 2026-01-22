import cli.GclCtl
import cli.commands.echo.Echo
import cli.commands.num_of_remote_replicas.NoRR
import cli.commands.num_of_remote_replicas.NoRRCommand
import com.github.ajalt.clikt.core.subcommands

fun main(args: Array<String>): kotlin.Unit =
    GclCtl()
        .subcommands(
            Echo(),
            NoRR().subcommands(
                NoRRCommand.GetNoRR,
                NoRRCommand.SetNoRR,
            )
        )
        .main(args)