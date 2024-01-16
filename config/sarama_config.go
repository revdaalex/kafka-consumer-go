package config

import (
	"crypto/tls"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func NewSaramaConfig(tlsEnable bool, tlsSkipVerify bool) *sarama.Config {
	cfg := sarama.NewConfig()

	host, _ := os.Hostname()

	cfg.ClientID = host
	cfg.Version = sarama.V0_11_0_2
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Session.Timeout = 20 * time.Second

	cfg.Producer.Return.Successes = true

	if tlsEnable {
		cfg.Net.TLS.Enable = true
		// #nosec G402
		cfg.Net.TLS.Config = &tls.Config{InsecureSkipVerify: tlsSkipVerify}
	}

	return cfg
}
