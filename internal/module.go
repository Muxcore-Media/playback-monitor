package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/contracts-playback/events"
	"github.com/Muxcore-Media/core/pkg/contracts"
	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const moduleVersion = "0.1.0"

type Module struct {
	monitorv1.UnimplementedPlaybackMonitorServiceServer
	httpLis              net.Listener
	grpcLis              net.Listener
	liveHub              *liveHub
	db                   *sql.DB
	stopCh               chan struct{}
	mc                   *client.Client
	grpcSrv              *grpc.Server
	httpAddr             string
	grpcAddr             string
	dbPath               string
	id                   string
	dbDialect            dbDialect
	publicAPIKey         string
	httpToken            string
	geoIPMode            string
	geoIPDBPath          string
	historyRetentionDays int
	activeSessionTimeout time.Duration
	cfgMu                sync.RWMutex
	mu                   sync.RWMutex
	notifyOnSessionStart bool
	notifyOnSessionStop  bool
	geoIPEnabled         bool
}

type Config struct {
	ID       string
	DBPath   string
	GRPCAddr string
	HTTPAddr string
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "playback-monitor"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "/var/lib/muxcore-playback-monitor/monitor.db"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9560"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8560"
	}
	if v := os.Getenv("PLAYBACK_MONITOR_DB_PATH"); v != "" {
		cfg.DBPath = v
	}
	if v := os.Getenv("PLAYBACK_MONITOR_GRPC_ADDR"); v != "" {
		cfg.GRPCAddr = v
	}
	if v := os.Getenv("PLAYBACK_MONITOR_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	notifyStart := envTruthy(os.Getenv("PLAYBACK_MONITOR_NOTIFY_ON_START"))
	notifyStop := envTruthy(os.Getenv("PLAYBACK_MONITOR_NOTIFY_ON_STOP"))
	geoMode, geoDB, geoEnabled := geoConfigFromEnv()
	publicKey := strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_PUBLIC_API_KEY"))
	httpToken := strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_HTTP_TOKEN"))
	retentionDays := envIntDefault(os.Getenv("PLAYBACK_MONITOR_HISTORY_RETENTION_DAYS"), 0)
	activeTimeout := envDurationDefault(os.Getenv("PLAYBACK_MONITOR_ACTIVE_TIMEOUT"), 15*time.Minute)
	return &Module{
		id:                   cfg.ID,
		dbPath:               cfg.DBPath,
		grpcAddr:             cfg.GRPCAddr,
		httpAddr:             cfg.HTTPAddr,
		stopCh:               make(chan struct{}),
		notifyOnSessionStart: notifyStart,
		notifyOnSessionStop:  notifyStop,
		geoIPMode:            geoMode,
		geoIPDBPath:          geoDB,
		geoIPEnabled:         geoEnabled,
		publicAPIKey:         publicKey,
		httpToken:            httpToken,
		historyRetentionDays: retentionDays,
		activeSessionTimeout: activeTimeout,
		liveHub:              newLiveHub(),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:          m.id,
		Name:        "Playback Monitor",
		Version:     moduleVersion,
		Roles:       []string{"analytics"},
		Description: "Session history, live activity, and playback analytics (Tautulli / Tracearr core)",
		Author:      "MuxCore",
		Capabilities: []string{
			"playback.monitor",
			"playback.analytics",
			"settings",
		},
		MinCoreVersion: "0.5.0",
		HTTPAddr:       m.httpAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	if err := m.initDB(ctx); err != nil {
		return err
	}
	if err := m.loadDurableSettings(); err != nil {
		slog.Warn("playback-monitor: load settings failed", "error", err)
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.grpcLis = lis
	httpLis, err := lc.Listen(ctx, "tcp", m.httpAddr)
	if err != nil {
		_ = lis.Close()
		return fmt.Errorf("listen HTTP %s: %w", m.httpAddr, err)
	}
	m.httpLis = httpLis
	slog.Info("playback-monitor initialized", "db", m.dbPath, "grpc", m.grpcAddr, "http", m.httpAddr)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	m.grpcSrv = grpc.NewServer()
	monitorv1.RegisterPlaybackMonitorServiceServer(m.grpcSrv, m)
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)
	go func() {
		slog.Info("playback-monitor gRPC started", "addr", m.grpcAddr)
		if err := m.grpcSrv.Serve(m.grpcLis); err != nil {
			slog.Error("playback-monitor gRPC error", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := m.Health(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable","error":` + jsonString(err.Error()) + `}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /sessions/active", m.withOperatorAuth(m.handleListActive))
	mux.HandleFunc("GET /history", m.withOperatorAuth(m.handleListHistory))
	mux.HandleFunc("GET /stats/home", m.withOperatorAuth(m.handleHomeStats))
	mux.HandleFunc("GET /stats/plays-by-date", m.withOperatorAuth(m.handlePlaysByDateHTTP))
	mux.HandleFunc("GET /stats/plays-by-hour", m.withOperatorAuth(m.handlePlaysByHourHTTP))
	mux.HandleFunc("GET /stats/plays-by-dow", m.withOperatorAuth(m.handlePlaysByDayOfWeekHTTP))
	mux.HandleFunc("GET /stats/plays-by-month", m.withOperatorAuth(m.handlePlaysByMonthHTTP))
	mux.HandleFunc("GET /stats/plays-by-stream-type", m.withOperatorAuth(m.handlePlaysByStreamTypeHTTP))
	mux.HandleFunc("GET /stats/plays-by-stream-resolution", m.withOperatorAuth(m.handlePlaysByStreamResolutionHTTP))
	mux.HandleFunc("GET /stats/plays-by-top-users", m.withOperatorAuth(m.handlePlaysByTopUsersHTTP))
	mux.HandleFunc("GET /stats/plays-by-top-platforms", m.withOperatorAuth(m.handlePlaysByTopPlatformsHTTP))
	mux.HandleFunc("GET /stats/plays-by-source-resolution", m.withOperatorAuth(m.handlePlaysBySourceResolutionHTTP))
	mux.HandleFunc("GET /stats/plays-by-platform-resolution", m.withOperatorAuth(m.handlePlaysByPlatformResolutionHTTP))
	mux.HandleFunc("GET /stats/concurrent-streams", m.withOperatorAuth(m.handleConcurrentStreamsHTTP))
	mux.HandleFunc("GET /stats/libraries", m.withOperatorAuth(m.handleLibraryStatsHTTP))
	mux.HandleFunc("GET /library/duplicates", m.withOperatorAuth(m.handleLibraryDuplicatesHTTP))
	mux.HandleFunc("GET /library/stale", m.withOperatorAuth(m.handleLibraryStaleHTTP))
	mux.HandleFunc("GET /library/storage", m.withOperatorAuth(m.handleLibraryStorageHTTP))
	mux.HandleFunc("GET /library/storage/history", m.withOperatorAuth(m.handleLibraryStorageHistoryHTTP))
	mux.HandleFunc("GET /stats/top-content", m.withOperatorAuth(m.handleTopContentHTTP))
	mux.HandleFunc("POST /ingest", m.withOperatorAuth(m.handleIngestHTTP))
	mux.HandleFunc("GET /notification/rules", m.withOperatorAuth(m.handleListNotificationRules))
	mux.HandleFunc("POST /notification/rules", m.withOperatorAuth(m.handleUpsertNotificationRule))
	mux.HandleFunc("PUT /notification/rules/{id}", m.withOperatorAuth(m.handleUpsertNotificationRuleByID))
	mux.HandleFunc("DELETE /notification/rules/{id}", m.withOperatorAuth(m.handleDeleteNotificationRule))
	mux.HandleFunc("GET /notification/destinations", m.withOperatorAuth(m.handleListNotificationDestinations))
	mux.HandleFunc("POST /notification/destinations", m.withOperatorAuth(m.handleUpsertNotificationDestination))
	mux.HandleFunc("POST /notification/destinations/{id}/test", m.withOperatorAuth(m.handleTestNotificationDestination))
	mux.HandleFunc("PUT /notification/destinations/{id}", m.withOperatorAuth(m.handleUpsertNotificationDestinationByID))
	mux.HandleFunc("DELETE /notification/destinations/{id}", m.withOperatorAuth(m.handleDeleteNotificationDestination))
	mux.HandleFunc("POST /import/tautulli", m.withOperatorAuth(m.handleImportTautulliHTTP))
	mux.HandleFunc("POST /import/jellystat", m.withOperatorAuth(m.handleImportJellystatHTTP))
	mux.HandleFunc("GET /events/streams", m.withOperatorAuth(m.handleStreamEventsSSE))
	mux.HandleFunc("GET /api/v2/public/docs", m.handlePublicDocs)
	mux.HandleFunc("GET /api/v2/public/health", m.handlePublicHealth)
	mux.HandleFunc("GET /api/v2/public/servers", m.handlePublicServers)
	mux.HandleFunc("GET /api/v2/public/streams", m.handlePublicStreams)
	mux.HandleFunc("GET /api/v2/public/history", m.handlePublicHistory)
	mux.HandleFunc("GET /api/v2/public/stats/home", m.handlePublicHomeStats)
	mux.HandleFunc("GET /api/v2/public/stats/concurrent", m.handlePublicConcurrentStats)
	mux.HandleFunc("GET /api/v2/public/stats/plays/platforms", m.handlePublicPlaysByPlatforms)
	mux.HandleFunc("GET /api/v2/public/stats/plays/stream-type", m.handlePublicPlaysByStreamType)
	mux.HandleFunc("GET /api/v2/public/stats/plays/stream-resolution", m.handlePublicPlaysByStreamResolution)
	mux.HandleFunc("GET /api/v2/public/stats/plays/source-resolution", m.handlePublicPlaysBySourceResolution)
	mux.HandleFunc("GET /api/v2/public/stats/plays/platform-resolution", m.handlePublicPlaysByPlatformResolution)
	mux.HandleFunc("GET /api/v2/public/stats/plays/by-date", m.handlePublicPlaysByDate)
	mux.HandleFunc("GET /api/v2/public/stats/plays/by-hour", m.handlePublicPlaysByHour)
	mux.HandleFunc("GET /api/v2/public/stats/plays/by-dow", m.handlePublicPlaysByDayOfWeek)
	mux.HandleFunc("GET /api/v2/public/stats/plays/by-month", m.handlePublicPlaysByMonth)
	mux.HandleFunc("GET /api/v2/public/stats/plays/top-users", m.handlePublicPlaysByTopUsers)
	mux.HandleFunc("GET /api/v2/public/libraries", m.handlePublicLibraries)
	mux.HandleFunc("GET /api/v2/public/recently-added", m.handlePublicRecentlyAdded)
	mux.HandleFunc("GET /api/v2/public/library/duplicates", m.handlePublicLibraryDuplicates)
	mux.HandleFunc("GET /api/v2/public/library/stale", m.handlePublicLibraryStale)
	mux.HandleFunc("GET /api/v2/public/library/storage", m.handlePublicLibraryStorage)
	mux.HandleFunc("GET /api/v2/public/library/storage/history", m.handlePublicLibraryStorageHistory)
	mux.HandleFunc("GET /api/v2/public/stats/top-content", m.handlePublicTopContent)
	mux.HandleFunc("GET /api/v2/public/users", m.handlePublicUsers)
	mux.HandleFunc("GET /api/v2/public/users/{id}", m.handlePublicUserByID)
	mux.HandleFunc("GET /api/v2/public/users/{id}/stats", m.handlePublicUserStats)
	mux.HandleFunc("GET /api/v2/public/users/{id}/history", m.handlePublicUserHistory)
	mux.HandleFunc("GET /api/v2/public/media/{ref}", m.handlePublicMedia)
	mux.HandleFunc("GET /api/v2/public/media/{ref}/stats", m.handlePublicMediaStats)
	mux.HandleFunc("GET /api/v2/public/media/{ref}/watchers", m.handlePublicMediaWatchers)
	mux.HandleFunc("GET /api/v2/public/media/{ref}/history", m.handlePublicMediaHistory)
	mux.HandleFunc("GET /api/v2/public/media/{ref}/children", m.handlePublicMediaChildren)
	go func() {
		slog.Info("playback-monitor HTTP started", "addr", m.httpAddr)
		if err := http.Serve(m.httpLis, mux); err != nil && err != http.ErrServerClosed { //nolint:gosec // listener lifecycle managed by Stop
			slog.Error("playback-monitor HTTP error", "error", err)
		}
	}()

	go m.connectCoreAndSubscribe(ctx) //nolint:gosec // module lifecycle goroutine outlives Start call
	go m.historyRetentionLoop(ctx)    //nolint:gosec // module lifecycle goroutine outlives Start call
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpLis != nil {
		_ = m.httpLis.Close()
	}
	m.mu.Lock()
	mc := m.mc
	if m.db != nil {
		_ = m.db.Close()
		m.db = nil
	}
	m.mc = nil
	m.mu.Unlock()
	if mc != nil {
		_ = mc.Close()
	}
	slog.Info("playback-monitor stopped")
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("not initialized")
	}
	return db.PingContext(ctx)
}

func (m *Module) connectCoreAndSubscribe(ctx context.Context) {
	addr := os.Getenv("MUXCORE_GRPC_ADDR")
	if addr == "" {
		return
	}
	var opts []client.Option
	if meshInsecure() {
		opts = append(opts, client.WithInsecure())
	}
	backoff := time.Second
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}
		c, err := client.Dial(addr, opts...)
		if err != nil {
			slog.Warn("playback-monitor: dial core failed, retrying", "error", err, "backoff", backoff)
			if !sleepUntilStop(m.stopCh, backoff) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		m.mu.Lock()
		if m.mc != nil {
			_ = m.mc.Close()
		}
		m.mc = c
		m.mu.Unlock()
		slog.Info("playback-monitor: connected to core mesh", "addr", addr)
		backoff = time.Second
		m.runMeshSubscriptions(ctx)
		select {
		case <-m.stopCh:
			return
		default:
		}
		slog.Warn("playback-monitor: mesh event subscriptions ended, reconnecting")
		if !sleepUntilStop(m.stopCh, backoff) {
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (m *Module) runMeshSubscriptions(ctx context.Context) {
	mc := m.eventClient()
	if mc == nil {
		return
	}
	var wg sync.WaitGroup
	active := 0
	for _, et := range []string{events.EventPlaybackStarted, events.EventPlaybackProgress, events.EventPlaybackStopped} {
		ch, cancel, err := mc.Events.Subscribe(ctx, et)
		if err != nil {
			slog.Debug("playback-monitor: subscribe failed", "type", et, "error", err)
			continue
		}
		active++
		wg.Add(1)
		go func(events <-chan *eventsv1.Event, eventType string, cancel context.CancelFunc) {
			defer wg.Done()
			defer cancel()
			for evt := range events {
				m.handlePlaybackEvent(ctx, eventType, evt)
			}
		}(ch, et, cancel)
	}
	if ch, cancel, err := mc.Events.Subscribe(ctx, "playback.library.item"); err == nil {
		active++
		wg.Add(1)
		go func(events <-chan *eventsv1.Event, cancel context.CancelFunc) {
			defer wg.Done()
			defer cancel()
			for evt := range events {
				m.handleLibraryItemEvent(ctx, evt)
			}
		}(ch, cancel)
	} else {
		slog.Debug("playback-monitor: subscribe library catalog failed", "error", err)
	}
	if ch, cancel, err := mc.Events.Subscribe(ctx, "playback.guard.violation"); err == nil {
		active++
		wg.Add(1)
		go func(events <-chan *eventsv1.Event, cancel context.CancelFunc) {
			defer wg.Done()
			defer cancel()
			for evt := range events {
				m.handleGuardViolationEvent(ctx, evt)
			}
		}(ch, cancel)
	} else {
		slog.Debug("playback-monitor: subscribe guard violations failed", "error", err)
	}
	if active == 0 {
		return
	}
	wg.Wait()
}

func (m *Module) handlePlaybackEvent(ctx context.Context, eventType string, evt *eventsv1.Event) {
	if evt == nil || len(evt.Payload) == 0 {
		return
	}
	source := evt.Source
	if source == "" {
		source = "jellyfin"
	}
	ev, err := sessionEventFromPlaybackJSON(source, evt.Payload)
	if err != nil {
		slog.Debug("playback-monitor: bad playback payload", "error", err)
		return
	}
	if ev.EventType == "" {
		ev.EventType = eventType
	}
	sessionID, _, ingestErr := m.ingestSessionEvent(ctx, ev)
	if ingestErr != nil {
		slog.Debug("playback-monitor: ingest failed", "type", eventType, "error", ingestErr)
		return
	}
	m.publishStreamLiveEvent(ctx, eventType, sessionID)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (m *Module) eventClient() *client.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mc
}
