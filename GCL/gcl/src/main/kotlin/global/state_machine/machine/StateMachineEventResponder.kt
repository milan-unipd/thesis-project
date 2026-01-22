package global.state_machine.machine

interface StateMachineEventResponder {

    fun onLeader()
    fun onNotLeader()
}