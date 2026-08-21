package internal

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

func (m *Module) handleListNotificationRules(w http.ResponseWriter, r *http.Request) {
	rules, err := m.listNotificationRules(r.Context(), r.URL.Query().Get("event_type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(rules))
	for _, rule := range rules {
		out = append(out, notificationRuleToMap(rule))
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": out})
}

func (m *Module) handleUpsertNotificationRule(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req struct {
		ID              string                  `json:"id"`
		Name            string                  `json:"name"`
		Enabled         *bool                   `json:"enabled"`
		EventType       string                  `json:"event_type"`
		TitleTemplate   string                  `json:"title_template"`
		MessageTemplate string                  `json:"message_template"`
		Severity        string                  `json:"severity"`
		Filters         NotificationRuleFilters `json:"filters"`
		DestinationIDs  []string                `json:"destination_ids"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	rule, err := m.upsertNotificationRule(r.Context(), NotificationRule{
		ID:              strings.TrimSpace(req.ID),
		Name:            strings.TrimSpace(req.Name),
		Enabled:         enabled,
		EventType:       strings.TrimSpace(req.EventType),
		TitleTemplate:   req.TitleTemplate,
		MessageTemplate: req.MessageTemplate,
		Severity:        strings.TrimSpace(req.Severity),
		Filters:         req.Filters,
		DestinationIDs:  req.DestinationIDs,
	})
	if err != nil {
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "unsupported") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rule": notificationRuleToMap(rule)})
}

func (m *Module) handleDeleteNotificationRule(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/notification/rules/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	if err := m.deleteNotificationRule(r.Context(), id); err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": id})
}

func (m *Module) handleUpsertNotificationRuleByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/notification/rules/")
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
		Name            string                  `json:"name"`
		Enabled         *bool                   `json:"enabled"`
		EventType       string                  `json:"event_type"`
		TitleTemplate   string                  `json:"title_template"`
		MessageTemplate string                  `json:"message_template"`
		Severity        string                  `json:"severity"`
		Filters         NotificationRuleFilters `json:"filters"`
		DestinationIDs  []string                `json:"destination_ids"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	rule, err := m.upsertNotificationRule(r.Context(), NotificationRule{
		ID:              id,
		Name:            strings.TrimSpace(req.Name),
		Enabled:         enabled,
		EventType:       strings.TrimSpace(req.EventType),
		TitleTemplate:   req.TitleTemplate,
		MessageTemplate: req.MessageTemplate,
		Severity:        strings.TrimSpace(req.Severity),
		Filters:         req.Filters,
		DestinationIDs:  req.DestinationIDs,
	})
	if err != nil {
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "unsupported") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rule": notificationRuleToMap(rule)})
}
