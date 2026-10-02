package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type routeInfo struct {
	Method        string `json:"method"`
	Path          string `json:"path"`
	Functionality string `json:"functionality"`
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {

	routes := []routeInfo{
		{
			Method:        "GET",
			Path:          "/",
			Functionality: "api methods, routes, and functionalities",
		},
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(routes); err != nil {
		slog.Error("failed to write home API response", "error", err)
		return
	}
}
