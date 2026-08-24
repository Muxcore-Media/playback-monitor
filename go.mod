module github.com/Muxcore-Media/playback-monitor

go 1.26.4

require (
	github.com/Muxcore-Media/contracts-notification v0.1.0
	github.com/Muxcore-Media/contracts-playback v0.1.0
	github.com/Muxcore-Media/core v0.5.8
	github.com/Muxcore-Media/core/pkg/contracts v0.5.8
	github.com/Muxcore-Media/core/sdk/go/client v0.5.8
	github.com/Muxcore-Media/core/sdk/go/module v0.5.8
	github.com/Muxcore-Media/playback-contract v0.1.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.5
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.11
	modernc.org/sqlite v1.55.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/Muxcore-Media/contracts-media v0.1.0 // indirect
	github.com/Muxcore-Media/core/pkg/tenant v0.5.8 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260610212136-7ab31c22f7ad // indirect
	modernc.org/libc v1.74.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/Muxcore-Media/contracts-notification => ../contracts-notification

replace github.com/Muxcore-Media/contracts-playback => ../contracts-playback

replace github.com/Muxcore-Media/playback-contract => ../playback-contract

replace github.com/Muxcore-Media/core => ../core

replace github.com/Muxcore-Media/core/pkg/contracts => ../core/pkg/contracts

replace github.com/Muxcore-Media/core/sdk/go/client => ../core/sdk/go/client

replace github.com/Muxcore-Media/core/sdk/go/module => ../core/sdk/go/module

replace github.com/Muxcore-Media/contracts-media => ../contracts-media

replace github.com/Muxcore-Media/core/pkg/tenant => ../core/pkg/tenant
