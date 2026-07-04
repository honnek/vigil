package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

func authMiddleware(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			tokenString, ok := strings.CutPrefix(authHeader, "Bearer ")
			if !ok {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			token, err := jwt.Parse(tokenString, func(t *jwt.Token) (any, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return secret, nil
			})
			if err != nil || !token.Valid {
				http.Error(w, "Invalid token", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
