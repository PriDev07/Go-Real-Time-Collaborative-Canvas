package models

// DrawEvent represents drawing a line on the canvas
type DrawEvent struct {
	Type  string  `json:"type"` // "draw", "clear", "init"
	X0    float64 `json:"x0,omitempty"`
	Y0    float64 `json:"y0,omitempty"`
	X1    float64 `json:"x1,omitempty"`
	Y1    float64 `json:"y1,omitempty"`
	Color string  `json:"color,omitempty"`
}
