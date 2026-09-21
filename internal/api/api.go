package api

import "net/http"

type ApiEngine struct {
	mux *http.ServeMux
}

func (api *ApiEngine) StartApi() {
	api.mux = http.NewServeMux()
	//api.mux.Handle("/", )
}

type HomeHandler struct {
}
