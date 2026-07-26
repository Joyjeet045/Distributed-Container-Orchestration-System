package api

import (
	"context"
	"net"

	grpcpb "minikube-orchestrator/api/proto"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpcHealth "google.golang.org/grpc/health/grpc_health_v1"
)

type GRPCServer struct {
	orch *service.Orchestrator
	grpcpb.UnimplementedOrchestratorServiceServer
}

func NewGRPCServer(orch *service.Orchestrator) *GRPCServer {
	return &GRPCServer{orch: orch}
}

func (s *GRPCServer) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	grpcHealth.RegisterHealthServer(server, health.NewServer())
	grpcpb.RegisterOrchestratorServiceServer(server, s)
	return server.Serve(lis)
}

func (s *GRPCServer) CreateDeployment(ctx context.Context, req *grpcpb.CreateDeploymentRequest) (*grpcpb.CreateDeploymentResponse, error) {
	dep, err := s.orch.CreateDeployment(ctx, fromPBDeploymentSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &grpcpb.CreateDeploymentResponse{Deployment: toPBDeployment(dep)}, nil
}

func (s *GRPCServer) DeleteDeployment(ctx context.Context, req *grpcpb.DeleteDeploymentRequest) (*grpcpb.DeleteDeploymentResponse, error) {
	if err := s.orch.DeleteDeployment(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &grpcpb.DeleteDeploymentResponse{Ok: true}, nil
}

func (s *GRPCServer) ListDeployments(ctx context.Context, _ *grpcpb.ListDeploymentsRequest) (*grpcpb.ListDeploymentsResponse, error) {
	items, err := s.orch.ListDeployments(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*grpcpb.Deployment, 0, len(items))
	for _, item := range items {
		result = append(result, toPBDeployment(item))
	}
	return &grpcpb.ListDeploymentsResponse{Deployments: result}, nil
}

func (s *GRPCServer) GetClusterState(ctx context.Context, _ *grpcpb.ClusterStateRequest) (*grpcpb.ClusterStateResponse, error) {
	state, err := s.orch.ClusterState(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make([]*grpcpb.ClusterNode, 0, len(state.Nodes))
	for _, n := range state.Nodes {
		nodes = append(nodes, &grpcpb.ClusterNode{
			Id:                  n.ID,
			Status:              string(n.Status),
			AllocatableMilliCpu: n.Allocatable.MilliCPU,
			AllocatableMemoryMb: n.Allocatable.MemoryMB,
			UsedMilliCpu:        n.Used.MilliCPU,
			UsedMemoryMb:        n.Used.MemoryMB,
		})
	}
	return &grpcpb.ClusterStateResponse{Nodes: nodes}, nil
}

func fromPBDeploymentSpec(spec *grpcpb.DeploymentSpec) model.DeploymentSpec {
	if spec == nil {
		return model.DeploymentSpec{}
	}
	resources := model.Resource{}
	if spec.GetResources() != nil {
		resources = model.Resource{MilliCPU: spec.GetResources().GetMilliCpu(), MemoryMB: spec.GetResources().GetMemoryMb()}
	}
	labels := map[string]string{}
	for k, v := range spec.GetLabels() {
		labels[k] = v
	}
	tolerations := make([]model.Toleration, 0, len(spec.GetTolerations()))
	for _, t := range spec.GetTolerations() {
		tolerations = append(tolerations, model.Toleration{
			Key:      t.GetKey(),
			Value:    t.GetValue(),
			Effect:   t.GetEffect(),
			Operator: t.GetOperator(),
		})
	}
	return model.DeploymentSpec{
		Name:            spec.GetName(),
		Image:           spec.GetImage(),
		ImagePullSecret: spec.GetImagePullSecret(),
		Replicas:        int(spec.GetReplicas()),
		Priority:        int(spec.GetPriority()),
		Resources:       resources,
		Labels:          labels,
		Tolerations:     tolerations,
	}
}

func toPBDeployment(dep model.Deployment) *grpcpb.Deployment {
	labels := map[string]string{}
	for k, v := range dep.Spec.Labels {
		labels[k] = v
	}
	tolerations := make([]*grpcpb.Toleration, 0, len(dep.Spec.Tolerations))
	for _, t := range dep.Spec.Tolerations {
		tolerations = append(tolerations, &grpcpb.Toleration{
			Key:      t.Key,
			Value:    t.Value,
			Effect:   t.Effect,
			Operator: t.Operator,
		})
	}
	return &grpcpb.Deployment{
		Id: dep.ID,
		Spec: &grpcpb.DeploymentSpec{
			Name:            dep.Spec.Name,
			Image:           dep.Spec.Image,
			ImagePullSecret: dep.Spec.ImagePullSecret,
			Replicas:        int32(dep.Spec.Replicas),
			Priority:        int32(dep.Spec.Priority),
			Resources: &grpcpb.ResourceRequest{
				MilliCpu: dep.Spec.Resources.MilliCPU,
				MemoryMb: dep.Spec.Resources.MemoryMB,
			},
			Labels:      labels,
			Tolerations: tolerations,
		},
		Status: string(dep.Status),
	}
}
