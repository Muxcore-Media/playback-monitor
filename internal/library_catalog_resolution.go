package internal

func catalogVideoResolutionFromMap(raw map[string]any) string {
	label := stringField(raw, "video_resolution", "videoResolution", "VideoResolution")
	height := int(int64Field(raw, "video_height", "videoHeight", "height", "Height"))
	width := int(int64Field(raw, "video_width", "videoWidth", "width", "Width"))
	return normalizeStreamResolution(height, width, label)
}
