package gtm

import (
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/elastic/beats/packetbeat/protos"
	"github.com/elastic/beats/packetbeat/protos/tcp"
)

// gtmPlugin application level protocol analyzer plugin
type gtmPlugin struct {
	ports        protos.PortsConfig
	parserConfig parserConfig
	transConfig  transactionConfig
	pub          transPub
}

// Application Layer tcp stream data to be stored on tcp connection context.
type connection struct {
	streams [2]*stream
	trans   transactions
}

// Uni-directional tcp stream state for parsing messages.
type stream struct {
	parser parser
}

var (
	debugf = logp.MakeDebug("gtm")

	// use isDebug/isDetailed to guard debugf/detailedf to minimize allocations
	// (garbage collection) when debug log is disabled.
	isDebug = false
)

func init() {
	protos.Register("gtm", New)
}

// New create and initializes channelOrigMsg new gtm protocol analyzer instance.
func New(
	testMode bool,
	results protos.Reporter,
	cfg *common.Config,
) (protos.Plugin, error) {
	p := &gtmPlugin{}
	config := defaultConfig
	if !testMode {
		if err := cfg.Unpack(&config); err != nil {
			return nil, err
		}
	}

	if err := p.init(results, &config); err != nil {
		return nil, err
	}

	//if config.EnableLatency {
	//	if config.LatencyIndex == "" {
	//		fmt.Println("Not setting latencyindex in packetbeat.yaml")
	//	}
	//	if len(config.ESservers) == 0 {
	//		fmt.Println("Not setting esservers in packetbeat.yaml")
	//	}
	//	if len(config.LatencyIndex) > 0 && len(config.ESservers) > 0 {
	//		go tcp.InitCounterGtmPkt(config.LatencyIndex, config.EsUsername, config.EsPassword, config.ESservers)
	//	}
	//}

	//go tcp.InitCounterGtmPkt(config.LatencyIndex, config.EsUsername, config.EsPassword, config.ESservers)

	//Init Harvest Slow Latency
	//if config.EnableLatency {
	//	if config.LatencyIndex == "" {
	//		fmt.Println("Not setting latencyindex in packetbeat.yaml")
	//	}
	//	if len(config.ESservers) == 0 {
	//		fmt.Println("Not setting esservers in packetbeat.yaml")
	//	}
	//	if len(config.LatencyIndex) > 0 && len(config.ESservers) >0 {
	//		InitGtmLatencyRealtime(config.LatencyIndex, config.ESservers)
	//	}
	//}
	return p, nil
}

func (gp *gtmPlugin) init(results protos.Reporter, config *gtmConfig) error {
	if err := gp.setFromConfig(config); err != nil {
		return err
	}
	gp.pub.results = results

	isDebug = logp.IsDebug("http")
	return nil
}

func (gp *gtmPlugin) setFromConfig(config *gtmConfig) error {
	if len(config.Ports) == 0 {
		var ports []int
		for i := config.StartPort; i <= config.EndPort; i++ {
			ports = append(ports, i)
		}
		config.Ports = ports
	}
	println(config.Ports)

	// set module configuration
	if err := gp.ports.Set(config.Ports); err != nil {
		return err
	}

	// set parser configuration
	parser := &gp.parserConfig
	parser.maxBytes = tcp.TCPMaxDataInStream

	// set transaction correlator configuration
	trans := &gp.transConfig
	trans.transactionTimeout = config.TransactionTimeout

	// set transaction publisher configuration
	pub := &gp.pub
	pub.sendRequest = config.SendRequest
	pub.sendResponse = config.SendResponse

	return nil
}

// ConnectionTimeout returns the per stream connection timeout.
// Return <=0 to set default tcp module transaction timeout.
func (gp *gtmPlugin) ConnectionTimeout() time.Duration {
	return gp.transConfig.transactionTimeout
}

// GetPorts returns the ports numbers packets shall be processed for.
func (gp *gtmPlugin) GetPorts() []int {
	return gp.ports.Ports
}

// Parse processes channelOrigMsg TCP packet. Return nil if connection
// state shall be dropped (e.g. parser not in sync with tcp stream)
func (gp *gtmPlugin) Parse(
	pkt *protos.Packet,
	tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	defer logp.Recover("Parse gtmPlugin exception")

	conn := gp.ensureConnection(private)
	st := conn.streams[dir]
	if st == nil {
		st = &stream{}
		st.parser.init(&gp.parserConfig, func(msg *message) error {
			return conn.trans.onMessage(tcptuple.IPPort(), dir, msg)
		})
		conn.streams[dir] = st
	}

	if err := st.parser.feed(pkt.Ts, pkt.Payload); err != nil {
		debugf("%v, dropping TCP stream for error in direction %v.", err, dir)
		gp.onDropConnection(conn)
		return nil
	}
	return conn
}

// ReceivedFin handles TCP-FIN packet.
func (gp *gtmPlugin) ReceivedFin(
	tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	return private
}

// GapInStream handles lost packets in tcp-stream.
func (gp *gtmPlugin) GapInStream(tcptuple *common.TCPTuple, dir uint8,
	nbytes int,
	private protos.ProtocolData,
) (protos.ProtocolData, bool) {
	conn := getConnection(private)
	if conn != nil {
		gp.onDropConnection(conn)
	}

	return nil, true
}

// onDropConnection processes and optionally sends incomplete
// transaction in case of connection being dropped due to error
func (gp *gtmPlugin) onDropConnection(conn *connection) {
}

func (gp *gtmPlugin) ensureConnection(private protos.ProtocolData) *connection {
	conn := getConnection(private)
	if conn == nil {
		conn = &connection{}
		conn.trans.init(&gp.transConfig, gp.pub.onTransaction)
	}
	return conn
}

func (conn *connection) dropStreams() {
	conn.streams[0] = nil
	conn.streams[1] = nil
}

func getConnection(private protos.ProtocolData) *connection {
	if private == nil {
		return nil
	}

	priv, ok := private.(*connection)
	if !ok {
		logp.Warn("gtm connection type error")
		return nil
	}
	if priv == nil {
		logp.Warn("Unexpected: gtm connection data not set")
		return nil
	}
	return priv
}
