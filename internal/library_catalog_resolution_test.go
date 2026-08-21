package internal

import "testing"

func TestCatalogVideoResolutionFromMap(t *testing.T) {
	got := catalogVideoResolutionFromMap(map[string]any{
		"height": float64(1080),
		"width":  float64(1920),
	})
	if got != "1080" {
		t.Fatalf("height map: got %q", got)
	}
	got = catalogVideoResolutionFromMap(map[string]any{
		"videoResolution": "4k",
	})
	if got != "4k" {
		t.Fatalf("label map: got %q", got)
	}
}
