package main

import "time"

const (
	TaskStaging  = "TASK_STAGING"
	TaskStarting = "TASK_STARTING"
	TaskRunning  = "TASK_RUNNING"
	TaskFinished = "TASK_FINISHED"
	TaskFailed   = "TASK_FAILED"
	TaskKilling  = "TASK_KILLING"
	TaskKilled   = "TASK_KILLED"
	TaskLost     = "TASK_LOST"
)

type AppResponse struct {
	App struct {
		ID   string      `json:"id"`
		Cmd  interface{} `json:"cmd"`
		Args []string    `json:"args"`
		User interface{} `json:"user"`
		Env  struct {
			HAPROXYGLOBALDEFAULTOPTIONS string `json:"HAPROXY_GLOBAL_DEFAULT_OPTIONS"`
			HAPROXYSSLCERT              string `json:"HAPROXY_SSL_CERT"`
			HAPROXYSYSCTLPARAMS         string `json:"HAPROXY_SYSCTL_PARAMS"`
		} `json:"env"`
		Instances             int           `json:"instances"`
		Cpus                  int           `json:"cpus"`
		Mem                   int           `json:"mem"`
		Disk                  int           `json:"disk"`
		Gpus                  int           `json:"gpus"`
		Executor              string        `json:"executor"`
		Constraints           [][]string    `json:"constraints"`
		Uris                  []interface{} `json:"uris"`
		Fetch                 []interface{} `json:"fetch"`
		StoreUrls             []interface{} `json:"storeUrls"`
		BackoffSeconds        int           `json:"backoffSeconds"`
		BackoffFactor         float64       `json:"backoffFactor"`
		MaxLaunchDelaySeconds int           `json:"maxLaunchDelaySeconds"`
		Container             struct {
			Type    string        `json:"type"`
			Volumes []interface{} `json:"volumes"`
			Docker  struct {
				Image          string        `json:"image"`
				Network        string        `json:"network"`
				PortMappings   []interface{} `json:"portMappings"`
				Privileged     bool          `json:"privileged"`
				Parameters     []interface{} `json:"parameters"`
				ForcePullImage bool          `json:"forcePullImage"`
			} `json:"docker"`
		} `json:"container"`
		HealthChecks []struct {
			GracePeriodSeconds     int    `json:"gracePeriodSeconds"`
			IntervalSeconds        int    `json:"intervalSeconds"`
			TimeoutSeconds         int    `json:"timeoutSeconds"`
			MaxConsecutiveFailures int    `json:"maxConsecutiveFailures"`
			PortIndex              int    `json:"portIndex"`
			Path                   string `json:"path"`
			Protocol               string `json:"protocol"`
			IgnoreHTTP1Xx          bool   `json:"ignoreHttp1xx"`
		} `json:"healthChecks"`
		ReadinessChecks []interface{} `json:"readinessChecks"`
		Dependencies    []interface{} `json:"dependencies"`
		UpgradeStrategy struct {
			MinimumHealthCapacity float64 `json:"minimumHealthCapacity"`
			MaximumOverCapacity   float64 `json:"maximumOverCapacity"`
		} `json:"upgradeStrategy"`
		Labels struct {
			DCOSPACKAGERELEASE         string `json:"DCOS_PACKAGE_RELEASE"`
			DCOSPACKAGESOURCE          string `json:"DCOS_PACKAGE_SOURCE"`
			DCOSPACKAGEMETADATA        string `json:"DCOS_PACKAGE_METADATA"`
			DCOSPACKAGEREGISTRYVERSION string `json:"DCOS_PACKAGE_REGISTRY_VERSION"`
			DCOSPACKAGEVERSION         string `json:"DCOS_PACKAGE_VERSION"`
			DCOSPACKAGENAME            string `json:"DCOS_PACKAGE_NAME"`
			DCOSPACKAGEISFRAMEWORK     string `json:"DCOS_PACKAGE_IS_FRAMEWORK"`
		} `json:"labels"`
		IPAddress interface{} `json:"ipAddress"`
		Version   time.Time   `json:"version"`
		Residency interface{} `json:"residency"`
		Secrets   struct {
		} `json:"secrets"`
		TaskKillGracePeriodSeconds interface{} `json:"taskKillGracePeriodSeconds"`
		UnreachableStrategy        struct {
			InactiveAfterSeconds int `json:"inactiveAfterSeconds"`
			ExpungeAfterSeconds  int `json:"expungeAfterSeconds"`
		} `json:"unreachableStrategy"`
		KillSelection         string   `json:"killSelection"`
		AcceptedResourceRoles []string `json:"acceptedResourceRoles"`
		Ports                 []int    `json:"ports"`
		PortDefinitions       []struct {
			Port     int    `json:"port"`
			Protocol string `json:"protocol"`
			Labels   struct {
			} `json:"labels"`
		} `json:"portDefinitions"`
		RequirePorts bool `json:"requirePorts"`
		VersionInfo  struct {
			LastScalingAt      time.Time `json:"lastScalingAt"`
			LastConfigChangeAt time.Time `json:"lastConfigChangeAt"`
		} `json:"versionInfo"`
		TasksStaged    int           `json:"tasksStaged"`
		TasksRunning   int           `json:"tasksRunning"`
		TasksHealthy   int           `json:"tasksHealthy"`
		TasksUnhealthy int           `json:"tasksUnhealthy"`
		Deployments    []interface{} `json:"deployments"`
		Tasks          []struct {
			IPAddresses []struct {
				IPAddress string `json:"ipAddress"`
				Protocol  string `json:"protocol"`
			} `json:"ipAddresses"`
			StagedAt           time.Time `json:"stagedAt"`
			State              string    `json:"state"`
			Ports              []int     `json:"ports"`
			StartedAt          time.Time `json:"startedAt"`
			Version            time.Time `json:"version"`
			ID                 string    `json:"id"`
			AppID              string    `json:"appId"`
			SlaveID            string    `json:"slaveId"`
			Host               string    `json:"host"`
			HealthCheckResults []struct {
				Alive               bool        `json:"alive"`
				ConsecutiveFailures int         `json:"consecutiveFailures"`
				FirstSuccess        time.Time   `json:"firstSuccess"`
				LastFailure         interface{} `json:"lastFailure"`
				LastSuccess         time.Time   `json:"lastSuccess"`
				LastFailureCause    interface{} `json:"lastFailureCause"`
				InstanceID          string      `json:"instanceId"`
			} `json:"healthCheckResults"`
		} `json:"tasks"`
		LastTaskFailure struct {
			AppID     string    `json:"appId"`
			Host      string    `json:"host"`
			Message   string    `json:"message"`
			State     string    `json:"state"`
			TaskID    string    `json:"taskId"`
			Timestamp time.Time `json:"timestamp"`
			Version   time.Time `json:"version"`
			SlaveID   string    `json:"slaveId"`
		} `json:"lastTaskFailure"`
	} `json:"app"`
}
