package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"

	"entropicworks.com/kafka-connect-healer/internal/config"
)

func basicAuth(next http.Handler, auth config.AuthConfiguration) http.Handler {
	if !auth.Enabled {
		return next
	}

	// Equal-length hashes avoid exposing credential lengths through comparison.
	expectedUsername := sha256.Sum256([]byte(auth.Username))
	expectedPassword := sha256.Sum256([]byte(auth.Password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		usernameHash := sha256.Sum256([]byte(username))
		passwordHash := sha256.Sum256([]byte(password))
		usernameMatches := subtle.ConstantTimeCompare(usernameHash[:], expectedUsername[:])
		passwordMatches := subtle.ConstantTimeCompare(passwordHash[:], expectedPassword[:])
		if !ok || usernameMatches&passwordMatches != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="kafka-connect-healer", charset="UTF-8"`)
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
