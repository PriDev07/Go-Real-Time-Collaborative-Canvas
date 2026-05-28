package ws

import "sync"

// RoomManager acts as the global system overseeing multiple rooms
type Manager struct {
	rooms map[string]*Room
	mu    sync.Mutex
}

// NewManager creates a new server room manager
func NewManager() *Manager {
	return &Manager{
		rooms: make(map[string]*Room),
	}
}

// GetOrCreateRoom retrieves an existing room or makes a new one if it doesn't exist
func (m *Manager) GetOrCreateRoom(name string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()
	room, exists := m.rooms[name]
	if !exists {
		room = newRoom(name)
		m.rooms[name] = room
		go room.run() // Start the engine's event loop for this specific room
	}
	return room
}
