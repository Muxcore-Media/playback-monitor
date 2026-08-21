package internal

import (
	"net/http"
	"strconv"
	"strings"
)

func publicChartQueryLimits(r *http.Request) (days, limit int) {
	days, _ = strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
	return days, limit
}

func writePublicChartBuckets(w http.ResponseWriter, rows []chartBucketRow) {
	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		data = append(data, map[string]any{
			"label": row.Label,
			"count": row.Count,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func (m *Module) handlePublicPlaysByPlatforms(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsByTopPlatforms(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByStreamType(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := publicChartQueryLimits(r)
	rows, err := m.playsByStreamType(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByStreamResolution(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsByStreamResolution(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysBySourceResolution(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsBySourceResolution(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByPlatformResolution(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsByPlatformResolution(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByHour(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := publicChartQueryLimits(r)
	rows, err := m.playsByHour(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByDayOfWeek(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := publicChartQueryLimits(r)
	rows, err := m.playsByDayOfWeek(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByMonth(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := publicChartQueryLimits(r)
	rows, err := m.playsByMonth(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByTopUsers(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsByTopUsers(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writePublicChartBuckets(w, rows)
}

func (m *Module) handlePublicPlaysByDate(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := publicChartQueryLimits(r)
	userIDs, err := parseChartUserIDs(r.URL.Query().Get("user_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	grouping := strings.EqualFold(r.URL.Query().Get("grouping"), "1") || strings.EqualFold(r.URL.Query().Get("grouping"), "true")
	chart, err := m.playsByDateChart(r.Context(), playsByDateChartOpts{
		Days:     days,
		UserIDs:  userIDs,
		Grouping: grouping,
		YAxis:    r.URL.Query().Get("y_axis"),
	})
	if err != nil {
		if strings.Contains(err.Error(), "invalid y_axis") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, playsByDateChartToJSON(chart))
}

func (m *Module) handlePlaysBySourceResolutionHTTP(w http.ResponseWriter, r *http.Request) {
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsBySourceResolution(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByPlatformResolutionHTTP(w http.ResponseWriter, r *http.Request) {
	days, limit := publicChartQueryLimits(r)
	rows, err := m.playsByPlatformResolution(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}
