package plugin

import (
	"errors"
	"sync"
)

type Registration struct {
	Name      string                 `json:"name"`
	Type      string                 `json:"type"`
	Path      string                 `json:"path"`
	Isolation RuntimeIsolationPolicy `json:"isolation"`
	Enabled   bool                   `json:"enabled"`
}

type Manager struct {
	mu            sync.RWMutex
	registrations map[string]Registration
}

func NewManager() *Manager {
	return &Manager{registrations: map[string]Registration{}}
}

func (m *Manager) Register(r Registration) error {
	if r.Name == "" {
		return errors.New("plugin name is required")
	}
	if r.Type == "" {
		return errors.New("plugin type is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registrations[r.Name] = r
	return nil
}

func (m *Manager) List() []Registration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Registration, 0, len(m.registrations))
	for _, r := range m.registrations {
		out = append(out, r)
	}
	return out
}

func (m *Manager) Enable(name string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.registrations[name]
	if !ok {
		return errors.New("plugin not found")
	}
	r.Enabled = enabled
	m.registrations[name] = r
	return nil
}
