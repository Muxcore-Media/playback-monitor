package internal

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
)

type geoLocation struct {
	Country     string
	CountryCode string
	City        string
	Lat         float64
	Lon         float64
}

var (
	geoAttrRE   = regexp.MustCompile(`(\w+)="([^"]*)"`)
	geoCacheMu  sync.RWMutex
	geoCache    = map[string]geoLocation{}
	geoDBMu     sync.RWMutex
	geoDBReader *geoip2.Reader
	geoDBLoaded string
)

func geoConfigFromEnv() (mode, dbPath string, enabled bool) {
	dbPath = strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_GEOIP_DB"))
	raw := strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_GEOIP"))
	if strings.EqualFold(raw, "plex") {
		return "plex", dbPath, true
	}
	if dbPath != "" {
		return "mmdb", dbPath, true
	}
	return "", dbPath, false
}

func normalizeGeoIPMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "plex":
		return "plex"
	case "mmdb", "maxmind", "db":
		return "mmdb"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

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

	m.cfgMu.RLock()
	mode := m.geoIPMode
	dbPath := m.geoIPDBPath
	m.cfgMu.RUnlock()
	if mode == "" && dbPath != "" {
		mode = "mmdb"
	}

	var loc geoLocation
	switch mode {
	case "plex":
		loc = lookupPlexGeoIP(ctx, ip)
	default:
		loc = lookupMMDBGeoIP(dbPath, ip)
	}
	geoCacheMu.Lock()
	geoCache[ip] = loc
	geoCacheMu.Unlock()
	return loc
}

func lookupMMDBGeoIP(dbPath, ip string) geoLocation {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return geoLocation{}
	}
	reader, err := geoipReader(dbPath)
	if err != nil {
		return geoLocation{}
	}
	parsed := net.ParseIP(strings.TrimSpace(ip))
	if parsed == nil {
		return geoLocation{}
	}
	record, err := reader.City(parsed)
	if err != nil {
		return geoLocation{}
	}
	loc := geoLocation{
		CountryCode: record.Country.IsoCode,
		Country:     record.Country.Names["en"],
		City:        record.City.Names["en"],
		Lat:         record.Location.Latitude,
		Lon:         record.Location.Longitude,
	}
	if loc.Country == "" && loc.CountryCode != "" {
		loc.Country = loc.CountryCode
	}
	return loc
}

func geoipReader(dbPath string) (*geoip2.Reader, error) {
	geoDBMu.RLock()
	if geoDBReader != nil && geoDBLoaded == dbPath {
		r := geoDBReader
		geoDBMu.RUnlock()
		return r, nil
	}
	geoDBMu.RUnlock()

	geoDBMu.Lock()
	defer geoDBMu.Unlock()
	if geoDBReader != nil && geoDBLoaded == dbPath {
		return geoDBReader, nil
	}
	if geoDBReader != nil {
		_ = geoDBReader.Close()
		geoDBReader = nil
		geoDBLoaded = ""
	}
	reader, err := geoip2.Open(dbPath)
	if err != nil {
		return nil, err
	}
	geoDBReader = reader
	geoDBLoaded = dbPath
	return reader, nil
}

func lookupPlexGeoIP(ctx context.Context, ip string) geoLocation {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := "https://plex.tv/api/v2/geoip?ip_address=" + strings.ReplaceAll(ip, " ", "%20")
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, http.NoBody)
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

func envIntDefault(raw string, def int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func envDurationDefault(raw string, def time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return d
}
