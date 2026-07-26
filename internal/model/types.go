package model

import "time"

type Resource struct {
	MilliCPU int64 `json:"milliCPU"`
	MemoryMB int64 `json:"memoryMB"`
}

type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Effect string `json:"effect"`
}

type Toleration struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Effect   string `json:"effect"`
	Operator string `json:"operator,omitempty"`
}

type NodeStatus string

const (
	NodeReady   NodeStatus = "Ready"
	NodeUnknown NodeStatus = "Unknown"
	NodeDown    NodeStatus = "Down"
)

type NodeFailureClass string

const (
	NodeFailureHealthy  NodeFailureClass = "Healthy"
	NodeFailureWarning  NodeFailureClass = "Warning"
	NodeFailureCritical NodeFailureClass = "Critical"
)

type NodeHealth struct {
	CPUUtilization       float64          `json:"cpuUtilization"`
	MemoryUtilization    float64          `json:"memoryUtilization"`
	LastHeartbeatAgeSec  int64            `json:"lastHeartbeatAgeSec"`
	ConsecutiveMissedTTL int              `json:"consecutiveMissedTTL"`
	FailureClass         NodeFailureClass `json:"failureClass"`
	Reason               string           `json:"reason,omitempty"`
	Isolated             bool             `json:"isolated"`
	Draining             bool             `json:"draining"`
	LastEvaluatedAt      time.Time        `json:"lastEvaluatedAt"`
}

type Node struct {
	ID          string            `json:"id"`
	Address     string            `json:"address"`
	Labels      map[string]string `json:"labels"`
	Taints      []Taint           `json:"taints,omitempty"`
	Capacity    Resource          `json:"capacity"`
	Allocatable Resource          `json:"allocatable"`
	Used        Resource          `json:"used"`
	Status      NodeStatus        `json:"status"`
	Health      NodeHealth        `json:"health"`
	LastSeen    time.Time         `json:"lastSeen"`
}

type DeploymentStatus string

const (
	DeploymentPending DeploymentStatus = "Pending"
	DeploymentRunning DeploymentStatus = "Running"
	DeploymentFailed  DeploymentStatus = "Failed"
)

type DeploymentSpec struct {
	Namespace       string            `json:"namespace,omitempty"`
	Name            string            `json:"name"`
	Image           string            `json:"image"`
	ImagePullSecret string            `json:"imagePullSecret,omitempty"`
	SecretMounts    []SecretMountSpec `json:"secretMounts,omitempty"`
	VolumeClaims    []string          `json:"volumeClaims,omitempty"`
	Replicas        int               `json:"replicas"`
	Priority        int               `json:"priority,omitempty"`
	Resources       Resource          `json:"resources"`
	Labels          map[string]string `json:"labels"`
	Tolerations     []Toleration      `json:"tolerations,omitempty"`
	LivenessProbe   ProbeSpec         `json:"livenessProbe,omitempty"`
	ReadinessProbe  ProbeSpec         `json:"readinessProbe,omitempty"`
	Rollout         RolloutSpec       `json:"rollout,omitempty"`
}

type RolloutStrategyType string

const (
	RolloutStrategyRollingUpdate RolloutStrategyType = "RollingUpdate"
	RolloutStrategyCanary        RolloutStrategyType = "Canary"
)

type RollingUpdateSpec struct {
	MaxUnavailable int `json:"maxUnavailable,omitempty"`
	MaxSurge       int `json:"maxSurge,omitempty"`
}

type CanarySpec struct {
	Enabled    bool `json:"enabled,omitempty"`
	Percentage int  `json:"percentage,omitempty"`
}

type RolloutSpec struct {
	Strategy                RolloutStrategyType `json:"strategy,omitempty"`
	RollingUpdate           RollingUpdateSpec   `json:"rollingUpdate,omitempty"`
	Canary                  CanarySpec          `json:"canary,omitempty"`
	ProgressDeadlineSeconds int                 `json:"progressDeadlineSeconds,omitempty"`
	AutoRollbackOnFailure   bool                `json:"autoRollbackOnFailure,omitempty"`
	MaxFailedWorkloads      int                 `json:"maxFailedWorkloads,omitempty"`
	SLOErrorRateThreshold   float64             `json:"sloErrorRateThreshold,omitempty"`
	SLOWindowSeconds        int                 `json:"sloWindowSeconds,omitempty"`
}

type ProbeSpec struct {
	Enabled             bool `json:"enabled"`
	InitialDelaySeconds int  `json:"initialDelaySeconds,omitempty"`
	FailureThreshold    int  `json:"failureThreshold,omitempty"`
}

type DeploymentConditionType string

const (
	ConditionAvailable   DeploymentConditionType = "Available"
	ConditionProgressing DeploymentConditionType = "Progressing"
	ConditionDegraded    DeploymentConditionType = "Degraded"
)

type DeploymentCondition struct {
	Type               DeploymentConditionType `json:"type"`
	Status             bool                    `json:"status"`
	Reason             string                  `json:"reason"`
	Message            string                  `json:"message"`
	LastTransitionTime time.Time               `json:"lastTransitionTime"`
}

type Deployment struct {
	ID                 string                  `json:"id"`
	Spec               DeploymentSpec          `json:"spec"`
	Generation         int64                   `json:"generation"`
	ObservedGeneration int64                   `json:"observedGeneration"`
	CurrentRevision    int                     `json:"currentRevision"`
	RevisionHistory    []DeploymentRevision    `json:"revisionHistory,omitempty"`
	RolloutStatus      DeploymentRolloutStatus `json:"rolloutStatus,omitempty"`
	Owner              string                  `json:"owner,omitempty"`
	Finalizers         []string                `json:"finalizers,omitempty"`
	DeletionTimestamp  *time.Time              `json:"deletionTimestamp,omitempty"`
	Status             DeploymentStatus        `json:"status"`
	Conditions         []DeploymentCondition   `json:"conditions,omitempty"`
	CreatedAt          time.Time               `json:"createdAt"`
	UpdatedAt          time.Time               `json:"updatedAt"`
}

type DeploymentRevision struct {
	Revision  int            `json:"revision"`
	Spec      DeploymentSpec `json:"spec"`
	CreatedAt time.Time      `json:"createdAt"`
}

type DeploymentRolloutStatus struct {
	Phase               string    `json:"phase,omitempty"`
	UpdatedReplicas     int       `json:"updatedReplicas"`
	ReadyReplicas       int       `json:"readyReplicas"`
	UnavailableReplicas int       `json:"unavailableReplicas"`
	Message             string    `json:"message,omitempty"`
	StartedAt           time.Time `json:"startedAt,omitempty"`
	LastUpdatedAt       time.Time `json:"lastUpdatedAt"`
}

type WorkloadStatus string

const (
	WorkloadPending     WorkloadStatus = "Pending"
	WorkloadRunning     WorkloadStatus = "Running"
	WorkloadFailed      WorkloadStatus = "Failed"
	WorkloadTerminating WorkloadStatus = "Terminating"
	WorkloadTerminated  WorkloadStatus = "Terminated"
)

type Workload struct {
	ID           string         `json:"id"`
	DeploymentID string         `json:"deploymentId"`
	Priority     int            `json:"priority,omitempty"`
	NodeID       string         `json:"nodeId"`
	Image        string         `json:"image"`
	Resources    Resource       `json:"resources"`
	Status       WorkloadStatus `json:"status"`
	ContainerID  string         `json:"containerId,omitempty"`
	RestartCount int            `json:"restartCount"`
	Version      int            `json:"version"`
	CreatedAt    time.Time      `json:"createdAt"`
	UpdatedAt    time.Time      `json:"updatedAt"`
}

type Assignment struct {
	Action          string        `json:"action,omitempty"`
	ID              string        `json:"id"`
	NodeID          string        `json:"nodeId"`
	WorkloadID      string        `json:"workloadId"`
	Image           string        `json:"image"`
	ImagePullSecret string        `json:"imagePullSecret,omitempty"`
	ContainerID     string        `json:"containerId,omitempty"`
	SecretFiles     []SecretFile  `json:"secretFiles,omitempty"`
	VolumeMounts    []VolumeMount `json:"volumeMounts,omitempty"`
	Resources       Resource      `json:"resources"`
}

type SecretMountSpec struct {
	SecretName string `json:"secretName"`
	MountPath  string `json:"mountPath"`
}

type SecretFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type VolumeMount struct {
	ClaimName string `json:"claimName"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly"`
}

type ServiceProtocol string

const (
	ServiceProtocolTCP ServiceProtocol = "TCP"
	ServiceProtocolUDP ServiceProtocol = "UDP"
)

type ServicePort struct {
	Name       string          `json:"name,omitempty"`
	Port       int             `json:"port"`
	TargetPort int             `json:"targetPort"`
	NodePort   int             `json:"nodePort,omitempty"`
	Protocol   ServiceProtocol `json:"protocol"`
}

type ServiceType string

const (
	ServiceTypeClusterIP ServiceType = "ClusterIP"
	ServiceTypeNodePort  ServiceType = "NodePort"
)

type ServiceSpec struct {
	Namespace string            `json:"namespace,omitempty"`
	Name      string            `json:"name"`
	Selector  map[string]string `json:"selector"`
	Ports     []ServicePort     `json:"ports"`
	Type      ServiceType       `json:"type,omitempty"`
}

type Namespace struct {
	Name      string    `json:"name"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type NamespaceQuota struct {
	Namespace      string    `json:"namespace"`
	MaxDeployments int       `json:"maxDeployments"`
	MaxMilliCPU    int64     `json:"maxMilliCPU"`
	MaxMemoryMB    int64     `json:"maxMemoryMB"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type Secret struct {
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Data      map[string]string `json:"data"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

type PersistentVolumePhase string

const (
	PersistentVolumeAvailable PersistentVolumePhase = "Available"
	PersistentVolumeBound     PersistentVolumePhase = "Bound"
	PersistentVolumeReleased  PersistentVolumePhase = "Released"
)

type PersistentVolume struct {
	Name            string                `json:"name"`
	CapacityMB      int64                 `json:"capacityMb"`
	StorageClass    string                `json:"storageClass,omitempty"`
	AccessMode      string                `json:"accessMode,omitempty"`
	ClaimNamespace  string                `json:"claimNamespace,omitempty"`
	ClaimName       string                `json:"claimName,omitempty"`
	BoundDeployment string                `json:"boundDeployment,omitempty"`
	Phase           PersistentVolumePhase `json:"phase"`
	CreatedAt       time.Time             `json:"createdAt"`
	UpdatedAt       time.Time             `json:"updatedAt"`
}

type PersistentVolumeClaimPhase string

const (
	PersistentVolumeClaimPending PersistentVolumeClaimPhase = "Pending"
	PersistentVolumeClaimBound   PersistentVolumeClaimPhase = "Bound"
)

type PersistentVolumeClaim struct {
	Namespace         string                     `json:"namespace"`
	Name              string                     `json:"name"`
	RequestedCapacity int64                      `json:"requestedCapacityMb"`
	StorageClass      string                     `json:"storageClass,omitempty"`
	AccessMode        string                     `json:"accessMode,omitempty"`
	VolumeName        string                     `json:"volumeName,omitempty"`
	Phase             PersistentVolumeClaimPhase `json:"phase"`
	CreatedAt         time.Time                  `json:"createdAt"`
	UpdatedAt         time.Time                  `json:"updatedAt"`
}

type NetworkPolicyPeer struct {
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type NetworkPolicyRule struct {
	From []NetworkPolicyPeer `json:"from,omitempty"`
	To   []NetworkPolicyPeer `json:"to,omitempty"`
	Port int                 `json:"port,omitempty"`
}

type NetworkPolicy struct {
	Namespace   string              `json:"namespace"`
	Name        string              `json:"name"`
	PodSelector map[string]string   `json:"podSelector"`
	Ingress     []NetworkPolicyRule `json:"ingress,omitempty"`
	Egress      []NetworkPolicyRule `json:"egress,omitempty"`
	Group       string              `json:"group,omitempty"`
	CreatedAt   time.Time           `json:"createdAt"`
	UpdatedAt   time.Time           `json:"updatedAt"`
}

type JobPhase string

const (
	JobPending   JobPhase = "Pending"
	JobRunning   JobPhase = "Running"
	JobSucceeded JobPhase = "Succeeded"
	JobFailed    JobPhase = "Failed"
)

type JobSpec struct {
	Namespace      string         `json:"namespace,omitempty"`
	Name           string         `json:"name"`
	Template       DeploymentSpec `json:"template"`
	Completions    int            `json:"completions,omitempty"`
	BackoffLimit   int            `json:"backoffLimit,omitempty"`
	TTLSeconds     int            `json:"ttlSeconds,omitempty"`
	HistoryLimit   int            `json:"historyLimit,omitempty"`
	ScheduleReason string         `json:"scheduleReason,omitempty"`
}

type Job struct {
	ID          string    `json:"id"`
	Spec        JobSpec   `json:"spec"`
	Status      JobPhase  `json:"status"`
	Deployment  string    `json:"deploymentId,omitempty"`
	Succeeded   int       `json:"succeeded"`
	Failed      int       `json:"failed"`
	RunCount    int       `json:"runCount"`
	LastRunAt   time.Time `json:"lastRunAt,omitempty"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type CronJob struct {
	Namespace      string    `json:"namespace"`
	Name           string    `json:"name"`
	ScheduleEveryS int       `json:"scheduleEverySeconds"`
	Template       JobSpec   `json:"template"`
	Suspend        bool      `json:"suspend"`
	HistoryLimit   int       `json:"historyLimit"`
	LastRunAt      time.Time `json:"lastRunAt,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type RoleBinding struct {
	Namespace string    `json:"namespace,omitempty"`
	Name      string    `json:"name"`
	Subject   string    `json:"subject"`
	Roles     []string  `json:"roles"`
	Inherits  []string  `json:"inherits,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type ControllerLease struct {
	LeaderID  string    `json:"leaderId"`
	ExpiresAt time.Time `json:"expiresAt"`
	Term      int64     `json:"term"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type ControlPlaneTransition struct {
	ID          string    `json:"id"`
	NodeID      string    `json:"nodeId"`
	Term        int64     `json:"term"`
	Action      string    `json:"action"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Service struct {
	ID                  string      `json:"id"`
	Spec                ServiceSpec `json:"spec"`
	ClusterIP           string      `json:"clusterIP,omitempty"`
	DNSRecordTTLSeconds int         `json:"dnsRecordTTLSeconds,omitempty"`
	CreatedAt           time.Time   `json:"createdAt"`
	UpdatedAt           time.Time   `json:"updatedAt"`
}

type ServiceEndpoint struct {
	ID         string          `json:"id"`
	ServiceID  string          `json:"serviceId"`
	WorkloadID string          `json:"workloadId"`
	NodeID     string          `json:"nodeId"`
	Address    string          `json:"address"`
	Port       int             `json:"port"`
	Protocol   ServiceProtocol `json:"protocol"`
	Ready      bool            `json:"ready"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

type DeploymentMetricSample struct {
	ID           string             `json:"id"`
	DeploymentID string             `json:"deploymentId"`
	Timestamp    time.Time          `json:"timestamp"`
	CPUUsage     float64            `json:"cpuUsage"`
	MemoryUsage  float64            `json:"memoryUsage"`
	Custom       map[string]float64 `json:"custom,omitempty"`
}

type AutoscalerPolicy struct {
	ID                        string    `json:"id"`
	DeploymentID              string    `json:"deploymentId"`
	MinReplicas               int       `json:"minReplicas"`
	MaxReplicas               int       `json:"maxReplicas"`
	TargetCPUUtilization      float64   `json:"targetCPUUtilization,omitempty"`
	TargetMemoryUtilization   float64   `json:"targetMemoryUtilization,omitempty"`
	CustomMetricName          string    `json:"customMetricName,omitempty"`
	TargetCustomMetricValue   float64   `json:"targetCustomMetricValue,omitempty"`
	PredictiveScalingEnabled  bool      `json:"predictiveScalingEnabled,omitempty"`
	PredictiveLookbackSamples int       `json:"predictiveLookbackSamples,omitempty"`
	PredictiveScaleFactor     float64   `json:"predictiveScaleFactor,omitempty"`
	StabilizationWindowSec    int       `json:"stabilizationWindowSec,omitempty"`
	ScaleUpCooldownSec        int       `json:"scaleUpCooldownSec,omitempty"`
	ScaleDownCooldownSec      int       `json:"scaleDownCooldownSec,omitempty"`
	LastScaleAt               time.Time `json:"lastScaleAt,omitempty"`
	LastScaleDirection        string    `json:"lastScaleDirection,omitempty"`
	CreatedAt                 time.Time `json:"createdAt"`
	UpdatedAt                 time.Time `json:"updatedAt"`
}

type ClusterState struct {
	Nodes       []Node       `json:"nodes"`
	Deployments []Deployment `json:"deployments"`
	Workloads   []Workload   `json:"workloads"`
}

type NodeHealthSample struct {
	NodeID            string           `json:"nodeId"`
	Timestamp         time.Time        `json:"timestamp"`
	CPUUtilization    float64          `json:"cpuUtilization"`
	MemoryUtilization float64          `json:"memoryUtilization"`
	FailureClass      NodeFailureClass `json:"failureClass"`
}

type NodeHealthTrend struct {
	NodeID               string           `json:"nodeId"`
	SampleCount          int              `json:"sampleCount"`
	AvgCPUUtilization    float64          `json:"avgCpuUtilization"`
	AvgMemoryUtilization float64          `json:"avgMemoryUtilization"`
	MaxCPUUtilization    float64          `json:"maxCpuUtilization"`
	MaxMemoryUtilization float64          `json:"maxMemoryUtilization"`
	CurrentFailureClass  NodeFailureClass `json:"currentFailureClass"`
	AnomalyDetected      bool             `json:"anomalyDetected"`
	AnomalyReason        string           `json:"anomalyReason,omitempty"`
	LastEvaluatedAt      time.Time        `json:"lastEvaluatedAt,omitempty"`
}

type EventLevel string

const (
	EventInfo  EventLevel = "Info"
	EventWarn  EventLevel = "Warn"
	EventError EventLevel = "Error"
)

type Event struct {
	ID           string     `json:"id"`
	Timestamp    time.Time  `json:"timestamp"`
	Level        EventLevel `json:"level"`
	Type         string     `json:"type"`
	Reason       string     `json:"reason"`
	Message      string     `json:"message"`
	DeploymentID string     `json:"deploymentId,omitempty"`
	WorkloadID   string     `json:"workloadId,omitempty"`
	NodeID       string     `json:"nodeId,omitempty"`
}
