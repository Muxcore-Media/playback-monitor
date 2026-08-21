package internal

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type geoLocation struct {
	Country     string
	CountryCode string
	City        string
	Lat         float64
	Lon         float64
}

var (
	geoAttrRE  = regexp.MustCompile(`(\w+)="([^"]*)"`)
	geoCacheMu sync.RWMutex
	geoCache   = map[string]geoLocation{}
)

func (m *Module) enrichSessionGeo(ctx context.Context, ev *SessionEvent) {
	if !m.geoIPEnabled || ev == nil {
		return
	}
	ip := strings.TrimSpace(ev.IPAddress)
	if ip == "" || ev.GeoCountry != "" {
		return
	}
	loc := m.lookupGeo(ctx, ip)
	if loc.Country == "" && loc.City == "" {
		return
	}
	ev.GeoCountry = firstNonEmpty(loc.CountryCode, loc.Country)
	ev.GeoCity = loc.City
	ev.GeoLat = loc.Lat
	ev.GeoLon = loc.Lon
}

func (m *Module) lookupGeo(ctx context.Context, ip string) geoLocation {
	if isPrivateIP(ip) {
		return geoLocation{Country: "Local Network"}
	}
	geoCacheMu.RLock()
	if cached, ok := geoCache[ip]; ok {
		geoCacheMu.RUnlock()
		return cached
	}
	geoCacheMu.RUnlock()

	loc := lookupPlexGeoIP(ctx, ip)
	geoCacheMu.Lock()
	geoCache[ip] = loc
	geoCacheMu.Unlock()
	return loc
}

func lookupPlexGeoIP(ctx context.Context, ip string) geoLocation {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := "https://plex.tv/api/v2/geoip?ip_address=" + strings.ReplaceAll(ip, " ", "%20")
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return geoLocation{}
	}
	req.Header.Set("Accept", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return geoLocation{}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8192))
	_ = resp.Body.Close()
	if err != nil {
		return geoLocation{}
	}
	return parsePlexGeoXML(string(body))
}

func parsePlexGeoXML(xml string) geoLocation {
	attrs := map[string]string{}
	for _, m := range geoAttrRE.FindAllStringSubmatch(xml, -1) {
		if len(m) == 3 {
			attrs[m[1]] = m[2]
		}
	}
	loc := geoLocation{
		CountryCode: attrs["code"],
		Country:     attrs["country"],
		City:        attrs["city"],
	}
	if coord := attrs["coordinates"]; coord != "" {
		parts := strings.Split(coord, ",")
		if len(parts) == 2 {
			if lat, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64); err == nil {
				loc.Lat = lat
			}
			if lon, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil {
				loc.Lon = lon
			}
		}
	}
	return loc
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(strings.TrimSpace(ipStr))
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}

const sessionSelectCols = `id, server_id, server_type, external_session_id, state, user_id, user_name, item_id, muxcore_id,
		imdb_id, tmdb_id, tvdb_id, title, media_type, started_at, stopped_at, last_progress_at, position_seconds, duration_seconds, is_transcode, play_method,
		platform, device, player, ip_address, source_module, geo_country, geo_city, geo_lat, geo_lon`

func scanSessionRecord(
	started, stopped, lastProgress *string,
	isTranscode *int,
	rec *SessionRecord,
) []any {
	return []any{
		&rec.ID, &rec.ServerID, &rec.ServerType, &rec.ExternalSessionID, &rec.State,
		&rec.UserID, &rec.UserName, &rec.ItemID, &rec.MuxcoreID, &rec.ImdbID, &rec.TmdbID, &rec.TvdbID,
		&rec.Title, &rec.MediaType,
		started, stopped, lastProgress, &rec.PositionSeconds, &rec.DurationSeconds, isTranscode, &rec.PlayMethod,
		&rec.Platform, &rec.Device, &rec.Player, &rec.IPAddress, &rec.SourceModule,
		&rec.GeoCountry, &rec.GeoCity, &rec.GeoLat, &rec.GeoLon,
	}
}

func geoInsertValues(ev SessionEvent) (country, city string, lat, lon float64) {
	return ev.GeoCountry, ev.GeoCity, ev.GeoLat, ev.GeoLon
}

func geoUpdateClause() string {
	return `geo_country = COALESCE(NULLIF(?, ''), geo_country),
			geo_city = COALESCE(NULLIF(?, ''), geo_city),
			geo_lat = CASE WHEN ? != 0 THEN ? ELSE geo_lat END,
			geo_lon = CASE WHEN ? != 0 THEN ? ELSE geo_lon END`
}

func geoUpdateArgs(ev SessionEvent) []any {
	return []any{ev.GeoCountry, ev.GeoCity, ev.GeoLat, ev.GeoLat, ev.GeoLon, ev.GeoLon}
}

func envGeoIPEnabled(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "plex", "on":
		return true
	default:
		return false
	}
}

func (m *Module) geoStatus() string {
	if !m.geoIPEnabled {
		return "disabled"
	}
	return fmt.Sprintf("plex (%d cached)", len(geoCache))
}
