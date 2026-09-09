package config

import "os"

// Config is shared by the reader and writer binaries; each uses the fields it
// needs. Everything comes from env vars with defaults matching deploy/docker-compose.yml.
type Config struct {
	SourceDSN   string // reader: replication connection to the source
	DestDSN     string // writer: destination database
	KafkaBroker string
	Topic       string
	Slot        string
	Publication string
}

func Load() Config {
	return Config{
		SourceDSN:   getenv("MINICDC_SOURCE_DSN", "postgres://postgres:minicdc@localhost:5410/terra"),
		DestDSN:     getenv("MINICDC_DEST_DSN", "postgres://postgres:minicdc@localhost:5411/warehouse"),
		KafkaBroker: getenv("MINICDC_KAFKA_BROKER", "localhost:19092"),
		Topic:       getenv("MINICDC_TOPIC", "cdc.events"),
		Slot:        getenv("MINICDC_SLOT", "minicdc"),
		Publication: getenv("MINICDC_PUBLICATION", "dbz_publication"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
