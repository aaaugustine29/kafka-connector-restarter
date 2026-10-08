package home

import (
	"encoding/json/v2"
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
		{
			Method:        "GET",
			Path:          "/config",
			Functionality: "retrieves current configuration",
		},
		{
			Method:        "PATCH",
			Path:          "/config",
			Functionality: "updates runtime configuration",
		},
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.MarshalWrite(w, routes); err != nil {
		slog.Error("failed to write home API response", "method", r.Method, "path", r.URL.Path, "error", err)
		return
	}
}
