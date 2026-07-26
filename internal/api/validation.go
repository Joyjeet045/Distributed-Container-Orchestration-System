package api

import (
	"fmt"
	"strings"

	"minikube-orchestrator/internal/model"
)

func validateNode(in model.Node) error {
	if strings.TrimSpace(in.ID) == "" {
		return fmt.Errorf("node id is required")
	}
	if in.Capacity.MilliCPU < 0 || in.Capacity.MemoryMB < 0 {
		return fmt.Errorf("node capacity cannot be negative")
	}
	if in.Allocatable.MilliCPU < 0 || in.Allocatable.MemoryMB < 0 {
		return fmt.Errorf("node allocatable cannot be negative")
	}
	return nil
}

func validateDeploymentSpec(in model.DeploymentSpec) error {
	if strings.TrimSpace(in.Name) == "" {
		return fmt.Errorf("deployment name is required")
	}
	if strings.TrimSpace(in.Image) == "" {
		return fmt.Errorf("deployment image is required")
	}
	if in.Replicas < 0 {
		return fmt.Errorf("replicas cannot be negative")
	}
	if in.Priority < 0 {
		return fmt.Errorf("priority cannot be negative")
	}
	if in.Resources.MilliCPU < 0 || in.Resources.MemoryMB < 0 {
		return fmt.Errorf("resource requests cannot be negative")
	}
	for _, t := range in.Tolerations {
		if strings.TrimSpace(t.Key) == "" {
			return fmt.Errorf("toleration key is required")
		}
	}
	if in.LivenessProbe.InitialDelaySeconds < 0 || in.ReadinessProbe.InitialDelaySeconds < 0 {
		return fmt.Errorf("probe initialDelaySeconds cannot be negative")
	}
	if in.LivenessProbe.FailureThreshold < 0 || in.ReadinessProbe.FailureThreshold < 0 {
		return fmt.Errorf("probe failureThreshold cannot be negative")
	}
	if in.Rollout.Strategy != "" && in.Rollout.Strategy != model.RolloutStrategyRollingUpdate && in.Rollout.Strategy != model.RolloutStrategyCanary {
		return fmt.Errorf("rollout strategy must be RollingUpdate or Canary")
	}
	if in.Rollout.RollingUpdate.MaxUnavailable < 0 || in.Rollout.RollingUpdate.MaxSurge < 0 {
		return fmt.Errorf("rollout maxUnavailable and maxSurge cannot be negative")
	}
	if in.Rollout.Canary.Enabled {
		if in.Rollout.Canary.Percentage <= 0 || in.Rollout.Canary.Percentage > 100 {
			return fmt.Errorf("canary percentage must be between 1 and 100")
		}
	}
	if in.Rollout.ProgressDeadlineSeconds < 0 {
		return fmt.Errorf("rollout progressDeadlineSeconds cannot be negative")
	}
	if in.Rollout.MaxFailedWorkloads < 0 {
		return fmt.Errorf("rollout maxFailedWorkloads cannot be negative")
	}
	return nil
}

func validateServiceSpec(in model.ServiceSpec) error {
	if strings.TrimSpace(in.Name) == "" {
		return fmt.Errorf("service name is required")
	}
	if len(in.Selector) == 0 {
		return fmt.Errorf("service selector is required")
	}
	if len(in.Ports) == 0 {
		return fmt.Errorf("service ports are required")
	}
	for _, p := range in.Ports {
		if p.Port <= 0 || p.Port > 65535 {
			return fmt.Errorf("service port must be between 1 and 65535")
		}
		if p.TargetPort <= 0 || p.TargetPort > 65535 {
			return fmt.Errorf("service targetPort must be between 1 and 65535")
		}
		if p.Protocol != model.ServiceProtocolTCP && p.Protocol != model.ServiceProtocolUDP {
			return fmt.Errorf("service protocol must be TCP or UDP")
		}
		if p.NodePort != 0 && (p.NodePort < 30000 || p.NodePort > 32767) {
			return fmt.Errorf("service nodePort must be between 30000 and 32767")
		}
	}
	if in.Type != "" && in.Type != model.ServiceTypeClusterIP && in.Type != model.ServiceTypeNodePort {
		return fmt.Errorf("service type must be ClusterIP or NodePort")
	}
	if in.Type == model.ServiceTypeNodePort {
		for _, p := range in.Ports {
			if p.NodePort == 0 {
				return fmt.Errorf("nodePort is required for NodePort services")
			}
		}
	}
	return nil
}

func validateAutoscalerPolicy(in model.AutoscalerPolicy) error {
	if strings.TrimSpace(in.DeploymentID) == "" {
		return fmt.Errorf("deploymentId is required")
	}
	if in.MinReplicas < 1 {
		return fmt.Errorf("minReplicas must be >= 1")
	}
	if in.MaxReplicas < in.MinReplicas {
		return fmt.Errorf("maxReplicas must be >= minReplicas")
	}
	if in.TargetCPUUtilization < 0 || in.TargetMemoryUtilization < 0 || in.TargetCustomMetricValue < 0 {
		return fmt.Errorf("target utilization values cannot be negative")
	}
	if in.CustomMetricName != "" && in.TargetCustomMetricValue <= 0 {
		return fmt.Errorf("targetCustomMetricValue must be > 0 when customMetricName is set")
	}
	if in.StabilizationWindowSec < 0 || in.ScaleUpCooldownSec < 0 || in.ScaleDownCooldownSec < 0 {
		return fmt.Errorf("stabilization and cooldown values cannot be negative")
	}
	if in.PredictiveLookbackSamples < 0 {
		return fmt.Errorf("predictiveLookbackSamples cannot be negative")
	}
	if in.PredictiveScaleFactor < 0 {
		return fmt.Errorf("predictiveScaleFactor cannot be negative")
	}
	if in.PredictiveScalingEnabled && in.PredictiveLookbackSamples == 1 {
		return fmt.Errorf("predictiveLookbackSamples must be >= 2 when predictiveScalingEnabled is true")
	}
	return nil
}

func validateDeploymentMetricSample(in model.DeploymentMetricSample) error {
	if strings.TrimSpace(in.DeploymentID) == "" {
		return fmt.Errorf("deploymentId is required")
	}
	if in.CPUUsage < 0 || in.MemoryUsage < 0 {
		return fmt.Errorf("cpuUsage and memoryUsage cannot be negative")
	}
	for k, v := range in.Custom {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("custom metric name cannot be empty")
		}
		if v < 0 {
			return fmt.Errorf("custom metric value cannot be negative")
		}
	}
	return nil
}
