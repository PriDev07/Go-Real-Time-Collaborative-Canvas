package ws

import (
	"encoding/json"
	"sync"
	"websockets_practise/internal/models"
)

// Room represents an isolated canvas lobby
type Room struct {
	name       string
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client

	// Canvas state history so new users getting to this room see what was drawn
	history []models.DrawEvent
	mu      sync.RWMutex // Protects the history array only
}

func newRoom(name string) *Room {
	return &Room{
		name:       name,
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
		history:    make([]models.DrawEvent, 0),
	}
}

// run is the central event loop for ONE room
func (r *Room) run() {
	for {
		select {
		case client := <-r.register:
			r.clients[client] = true

			// Send current canvas history to the new room member
			r.mu.RLock()
			if len(r.history) > 0 {
				historyData, _ := json.Marshal(map[string]interface{}{
					"type": "init",
					"data": r.history,
				})
				client.send <- historyData
			}
			r.mu.RUnlock()

		case client := <-r.unregister:
			if _, ok := r.clients[client]; ok {
				delete(r.clients, client)
				close(client.send)
			}
		case message := <-r.broadcast:
			// Process broadcasted DrawEvents to save to history
			var event models.DrawEvent
			if err := json.Unmarshal(message, &event); err == nil {
				r.mu.Lock()
				if event.Type == "clear" {
					r.history = make([]models.DrawEvent, 0)
				} else if event.Type == "draw" {
					r.history = append(r.history, event)
				}
				r.mu.Unlock()
			}

			// Broadcast to all clients inside this isolated room
			for client := range r.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(r.clients, client)
				}
			}
		}
	}
}
