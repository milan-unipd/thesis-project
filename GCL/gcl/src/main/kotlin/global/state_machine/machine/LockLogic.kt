package global.state_machine.machine

import global.state_machine.LockEntry
import global.state_machine.commands.LockCommand

internal fun GCLStateMachine.expireLocks() {
    locks.entries.removeIf {
        it.value.expiresAtTick <= logicalTime.get()
    }
}

internal fun GCLStateMachine.acquireLock(cmd: LockCommand.Acquire): String {
    expireLocks()
    val lockEntry = locks[cmd.lockId]
    return if (lockEntry == null) {
        val x = logicalTime.get() + cmd.leaseTicks
        locks[cmd.lockId] = LockEntry(
            cmd.ownerId,
            x
        )
        "ACQUIRED"
    } else if (lockEntry.ownerId == cmd.ownerId) {
        expireLocks()
        val entry = locks[cmd.lockId]
        if (entry?.ownerId == cmd.ownerId) {
            entry.expiresAtTick = logicalTime.get() + cmd.leaseTicks
            "RENEWED"
        } else {
            "NOT_OWNER"
        }
    } else {
        "BUSY"
    }
}

internal fun GCLStateMachine.releaseLock(cmd: LockCommand.Release): String {
    val entry = locks[cmd.lockId]
    return if (entry?.ownerId == cmd.ownerId) {
        locks.remove(cmd.lockId)
        "RELEASED"
    } else {
        "NOT_OWNER"
    }
}

