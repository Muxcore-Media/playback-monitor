package internal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

func (m *Module) handleListNotificationDestinations(w http.ResponseWriter, r *http.Request) {
	dests, err := m.listNotificationDestinations(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(dests))
	for _, dest := range dests {
		out = append(out, notificationDestinationToMap(dest))
	}
	writeJSON(w, http.StatusOK, map[string]any{"destinations": out})
}

func (m *Module) handleUpsertNotificationDestination(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req struct {
		ID      string            `json:"id"`
		Name    string            `json:"name"`
		Type    string            `json:"type"`
		Enabled *bool             `json:"enabled"`
		Config  map[string]string `json:"config"`
		Events  []string          `json:"events"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	dest, err := m.upsertNotificationDestination(r.Context(), NotificationDestination{
		ID:      strings.TrimSpace(req.ID),
		Name:    strings.TrimSpace(req.Name),
		Type:    strings.TrimSpace(req.Type),
		Enabled: enabled,
		Config:  req.Config,
		Events:  req.Events,
	})
	if err != nil {
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "unsupported") || strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "blocked") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"destination": notificationDestinationToMap(dest)})
}

func (m *Module) handleUpsertNotificationDestinationByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/notification/destinations/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req struct {
		Name    string            `json:"name"`
		Type    string            `json:"type"`
		Enabled *bool             `json:"enabled"`
		Config  map[string]string `json:"config"`
		Events  []string          `json:"events"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	existing, err := m.getNotificationDestination(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	config := req.Config
	if config == nil {
		config = existing.Config
	} else {
		for k, v := range existing.Config {
			if v == "" {
				continue
			}
			if cur := strings.TrimSpace(config[k]); cur == "" || cur == "********" {
				config[k] = v
			}
		}
	}
	dest, err := m.upsertNotificationDestination(r.Context(), NotificationDestination{
		ID:      id,
		Name:    firstNonEmptyStr(strings.TrimSpace(req.Name), existing.Name),
		Type:    firstNonEmptyStr(strings.TrimSpace(req.Type), existing.Type),
		Enabled: enabled,
		Config:  config,
		Events:  req.Events,
	})
	if err != nil {
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "unsupported") || strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "blocked") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"destination": notificationDestinationToMap(dest)})
}

func (m *Module) handleDeleteNotificationDestination(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/notification/destinations/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	if err := m.deleteNotificationDestination(r.Context(), id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": id})
}

func (m *Module) handleTestNotificationDestination(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/notification/destinations/")
	id = strings.TrimSuffix(id, "/test")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	if err := m.testNotificationDestination(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
