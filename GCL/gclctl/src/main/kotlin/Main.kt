import cli.GclCtl
import com.github.ajalt.clikt.core.subcommands
import cli.commands.echo.Echo
import cli.commands.num_of_remote_replicas.NoRR
import cli.commands.num_of_remote_replicas.NoRRCommand

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