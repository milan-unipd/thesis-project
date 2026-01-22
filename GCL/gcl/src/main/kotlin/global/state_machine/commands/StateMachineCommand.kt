package global.state_machine.commands

import kotlinx.serialization.Serializable
import org.apache.ratis.protocol.Message
import kotlin.text.Charsets.UTF_8

@Serializable
sealed class StateMachineCommand {

    fun serialize(): String {
        return serializeToByteArray().toString(UTF_8)
    }

    fun toMessage(): Message =
        Message.valueOf(serialize())

    fun serializeToByteArray(): ByteArray {
        return CommandCodec.serialize(this)
    }
}

