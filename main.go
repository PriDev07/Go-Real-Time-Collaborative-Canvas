// High-Concurrency Room-Based Canvas Server
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1024 * 10
)

// DrawEvent represents drawing a line on the canvas
type DrawEvent struct {
	Type  string  `json:"type"` // "draw", "clear", "init"
	X0    float64 `json:"x0,omitempty"`
	Y0    float64 `json:"y0,omitempty"`
	X1    float64 `json:"x1,omitempty"`
	Y1    float64 `json:"y1,omitempty"`
	Color string  `json:"color,omitempty"`
}

// Client represents a connected websocket connection attached to a room
type Client struct {
	room *Room
	conn *websocket.Conn
	send chan []byte // Buffered channel of outbound messages
}

// Room represents an isolated canvas lobby (similar to the previous Hub, but many can exist)
type Room struct {
	name       string
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client

	// Canvas state history so new users getting to this room see what was drawn
	history []DrawEvent
	mu      sync.RWMutex // Protects the history array only
}

func newRoom(name string) *Room {
	return &Room{
		name:       name,
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
		history:    make([]DrawEvent, 0),
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
			var event DrawEvent
			if err := json.Unmarshal(message, &event); err == nil {
				r.mu.Lock()
				if event.Type == "clear" {
					r.history = make([]DrawEvent, 0)
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

func (c *Client) readPump() {
	defer func() {
		c.room.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		c.room.broadcast <- message
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current websocket frame
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// RoomManager acts as the global system overseeing multiple rooms
type RoomManager struct {
	rooms map[string]*Room
	mu    sync.Mutex
}

func (m *RoomManager) getOrCreateRoom(name string) *Room {
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

func main() {
	manager := &RoomManager{
		rooms: make(map[string]*Room),
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// Detect room from query parameters ?room=abc
		roomName := r.URL.Query().Get("room")
		if roomName == "" {
			http.Error(w, "room query parameter is required", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}

		room := manager.getOrCreateRoom(roomName)
		client := &Client{room: room, conn: conn, send: make(chan []byte, 256)}

		// Join the room
		client.room.register <- client

		// Concurrently handle writing and reading to ensure single slow clients don't block others
		go client.writePump()
		go client.readPump()
	})

	fmt.Println("High-Concurrency Room-Based Canvas Server Started at: http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
