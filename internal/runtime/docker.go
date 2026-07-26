package runtime

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

type ContainerRuntime interface {
	RunWorkload(ctx context.Context, workloadID, imageRef, imagePullSecret string) (string, error)
	StopWorkload(ctx context.Context, workloadID, containerID string) error
	ListManagedWorkloads(ctx context.Context) (map[string]string, error)
}

type DockerRuntime struct {
	cli *client.Client
}

func NewDockerRuntime() (*DockerRuntime, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	return &DockerRuntime{cli: cli}, nil
}

func (d *DockerRuntime) RunWorkload(ctx context.Context, workloadID, imageRef, imagePullSecret string) (string, error) {
	pullCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	pullOpts := image.PullOptions{}
	if strings.TrimSpace(imagePullSecret) != "" {
		pullOpts.RegistryAuth = base64.StdEncoding.EncodeToString([]byte(imagePullSecret))
	}
	reader, err := d.cli.ImagePull(pullCtx, imageRef, pullOpts)
	if err != nil {
		return "", fmt.Errorf("image pull: %w", err)
	}
	_ = reader.Close()

	resp, err := d.cli.ContainerCreate(ctx, &container.Config{
		Image: imageRef,
		Cmd:   []string{"sleep", "3600"},
		Labels: map[string]string{
			"orchestrator.workload": workloadID,
		},
	}, nil, nil, nil, "orch-"+workloadID)
	if err != nil {
		return "", fmt.Errorf("container create: %w", err)
	}
	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("container start: %w", err)
	}
	return resp.ID, nil
}

func (d *DockerRuntime) StopWorkload(ctx context.Context, workloadID, containerID string) error {
	if strings.TrimSpace(containerID) == "" {
		managed, err := d.ListManagedWorkloads(ctx)
		if err != nil {
			return err
		}
		containerID = managed[workloadID]
		if strings.TrimSpace(containerID) == "" {
			return nil
		}
	}
	timeout := 10
	_ = d.cli.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout})
	_ = d.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})
	return nil
}

func (d *DockerRuntime) ListManagedWorkloads(ctx context.Context) (map[string]string, error) {
	args := filters.NewArgs()
	args.Add("label", "orchestrator.workload")
	containers, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, c := range containers {
		if workloadID, ok := c.Labels["orchestrator.workload"]; ok && strings.TrimSpace(workloadID) != "" {
			out[workloadID] = c.ID
		}
	}
	return out, nil
}
