package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"websockets_practise/internal/ws"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	manager := ws.NewManager()

	// Serve the static frontend folder natively
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// Detect room from query parameters ?room=abc
		roomName := r.URL.Query().Get("room")
		if roomName == "" {
			http.Error(w, "room query parameter is required", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("Error upgrading websocket:", err)
			return
		}

		room := manager.GetOrCreateRoom(roomName)
		client := ws.NewClient(room, conn)

		// Join the room
		client.JoinRoom()

		// Concurrently handle writing and reading to ensure single slow clients don't block others
		go client.WritePump()
		go client.ReadPump()
	})

	fmt.Println("High-Concurrency Room-Based Canvas Server Started at: http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
