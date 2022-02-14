package gtm

import (
	"github.com/elastic/beats/packetbeat/config"
	"github.com/elastic/beats/packetbeat/protos"
)

type gtmConfig struct {
	config.ProtocolCommon `config:",inline"`
	StartPort             int      `config:",startport"`
	EndPort               int      `config:"endport"`
	LatencyIndex          string   `config:"latencyindex"`
	EsUsername			  string   `config:"esusername"`
	EsPassword 			  string   `config:"espassword"`
	ESservers             []string `config:"esservers"`
	EnableLatency         bool     `config:"enablelatency"`
}

var (
	defaultConfig = gtmConfig{
		ProtocolCommon: config.ProtocolCommon{
			TransactionTimeout: protos.DefaultTransactionExpiration,
		},
	}
)

func (c *gtmConfig) Validate() error {
	return nil
}
