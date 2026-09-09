package writer

// destType maps a source type name (as normalized by the reader) to the
// destination column type. Source and destination are both Postgres in this
// project, so known types pass through; anything unexpected degrades to text,
// which loses type fidelity but never data.
func destType(sourceType string) string {
	switch sourceType {
	case "boolean", "smallint", "integer", "bigint",
		"real", "double precision", "numeric",
		"text", "date", "timestamp", "timestamptz",
		"uuid", "jsonb":
		return sourceType
	default:
		return "text"
	}
}
