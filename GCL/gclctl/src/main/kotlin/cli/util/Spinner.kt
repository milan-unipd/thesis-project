package cli.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch


class Spinner(
    private val message: String = "Working",
    private val intervalMs: Long = 100
) {
    private val frames = listOf("|", "/", "-", "\\")
    private var job: Job? = null

    fun start(scope: CoroutineScope) {
        job = scope.launch {
            var i = 0
            while (isActive) {
                print("\r$message ${frames[i++ % frames.size]}")
                delay(intervalMs)
            }
        }
    }

    suspend fun stop(doneMessage: String = "Done") {
        if (job?.isActive == true) {
            job?.cancelAndJoin()
            print("\r$doneMessage${" ".repeat(20)}\n")
        }
    }
}