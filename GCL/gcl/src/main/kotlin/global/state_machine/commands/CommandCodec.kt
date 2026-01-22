package global.state_machine.commands

import kotlinx.serialization.json.Json

object CommandCodec {

    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = false
        classDiscriminator = "type"
    }

    fun serialize(cmd: StateMachineCommand): ByteArray {
        return json.encodeToString(StateMachineCommand.serializer(), cmd).encodeToByteArray()
    }

    fun deserialize(bytes: ByteArray): StateMachineCommand =
        json.decodeFromString(
            StateMachineCommand.serializer(),
            bytes.decodeToString()
        )
}