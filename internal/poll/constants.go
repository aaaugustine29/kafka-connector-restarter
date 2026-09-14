package poll

const (
	defaultConnectorStatusPath  = "/connectors?expand=status"
	defaultTaskRestartPath      = "/connectors/%s/tasks/%d/restart"
	defaultConnectorRestartPath = "/connectors/%s/restart?includeTasks=true&onlyFailed=true"
)
