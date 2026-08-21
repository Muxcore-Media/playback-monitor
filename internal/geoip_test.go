package internal

import (
	"context"
	"testing"
)

func TestParsePlexGeoXML(t *testing.T) {
	xml := `<location code="US" country="United States" city="New York" subdivisions="New York" coordinates="40.7, -74.0"/>`
	loc := parsePlexGeoXML(xml)
	if loc.CountryCode != "US" || loc.City != "New York" {
		t.Fatalf("unexpected parse: %+v", loc)
	}
	if loc.Lat != 40.7 || loc.Lon != -74.0 {
		t.Fatalf("unexpected coords: %+v", loc)
	}
}

func TestIsPrivateIP(t *testing.T) {
	if !isPrivateIP("192.168.1.1") {
		t.Fatal("expected private")
	}
	if isPrivateIP("8.8.8.8") {
		t.Fatal("expected public")
	}
}

func TestEnrichSessionGeoPrivateIP(t *testing.T) {
	m := NewModule(Config{})
	m.geoIPEnabled = true
	ev := &SessionEvent{IPAddress: "10.0.0.5"}
	m.enrichSessionGeo(context.Background(), ev)
	if ev.GeoCountry != "Local Network" {
		t.Fatalf("expected local network, got %q", ev.GeoCountry)
	}
}