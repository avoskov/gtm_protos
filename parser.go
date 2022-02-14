package gtm

import (
	"errors"
	"fmt"
	"regexp"

	//"github.com/elastic/go-elasticsearch/esapi"

	//"fmt"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/streambuf"
	"github.com/elastic/beats/packetbeat/protos/applayer"

	"os"
	"strings"
	"time"
)

type parser struct {
	buf     streambuf.Buffer
	config  *parserConfig
	message *message

	onMessage func(m *message) error
}

type parserConfig struct {
	maxBytes int
}

type message struct {
	applayer.Message

	// indicator for parsed message being complete or requires more messages
	// (if false) to be merged to generate full message.
	isComplete bool

	// list element use by 'transactions' for correlation
	next        *message
	failed      bool
	content     common.NetString
	origMsg     string
	mrpcId      string
	origSysID   string
	targSysID   string
	origPrcID   string
	commandName string
	rsRetCode   string
	timePayload time.Time
}

// ErrStreamTooLarge Error code if stream exceeds max allowed size on append.
var (
	ErrStreamTooLarge = errors.New("Stream data too large")
)

func (p *parser) init(
	cfg *parserConfig,
	onMessage func(*message) error,
) {
	*p = parser{
		buf:       streambuf.Buffer{},
		config:    cfg,
		onMessage: onMessage,
	}
}

func (p *parser) append(data []byte) error {
	_, err := p.buf.Write(data)
	if err != nil {
		return err
	}

	if p.config.maxBytes > 0 && p.buf.Total() > p.config.maxBytes {
		return ErrStreamTooLarge
	}
	return nil
}

func (p *parser) feed(ts time.Time, data []byte) error {
	if err := p.append(data); err != nil {
		return err
	}

	if p.message == nil {
		p.message = p.newMessage(ts)
	}
	for p.buf.Total() > 0 {

		msg, err := p.parse()
		if err != nil {
			return err
		}
		if msg == nil {
			break // wait for more data
		}

		// reset buffer and message -> handle next message in buffer
		p.buf.Reset()
		p.message = nil

		// call message handler callback
		if err := p.onMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

func (p *parser) newMessage(ts time.Time) *message {
	return &message{
		Message: applayer.Message{
			Ts: ts,
		},
	}
}

func (p *parser) parse() (*message, error) {

	var (
		buf         []byte
		err         error
		isRequest   = true
		origMsgID   string
		mrpcId      string
		origSysID   string
		targSysID   string
		origPrcID   string
		commandName string
		rsRetCode   string
		timePayload string
		messageType string
	)

	msg := p.message
	buf, err = p.buf.CollectUntil([]byte("</message>"))

	if err == streambuf.ErrNoMoreBytes {
		return nil, nil
	}

	msg.Size = uint64(p.buf.BufferConsumed())

	dir := applayer.NetOriginalDirection
	if len(buf) > 0 {

		payload := string(buf)
		messageType = ExtractInnerTagXML("message type", payload)
		origMsgID = ExtractXML("origMsgID", payload)
		mrpcId = ExtractXML("field name=\"mrpcID\"", payload)
		origSysID = ExtractXML("origSysID", payload)
		targSysID = ExtractXML("targSysID", payload)
		origPrcID = ExtractXML("origPrcID", payload)
		timePayload = ExtractXML("timeStamp", payload)
		commandName = ExtractInnerTagXML("command name", payload)

		if strings.Contains(messageType, "REQST") {
			idx := strings.Index(payload, "<message")
			if idx < 0 {
				idx = 0
			}
			buf = buf[idx:]
			isRequest = true
		} else {
			rsRetCode = ExtractXML("retCode", payload)
			idx := strings.Index(payload, "<?xml")
			if idx < 0 {
				idx = 0
			}
			buf = buf[idx:]
			isRequest = false
		}
		if !isRequest {
			dir = applayer.NetReverseDirection
		} else {
			dir = applayer.NetOriginalDirection
		}
		emptyMprcid := strings.TrimSpace(mrpcId)
		if emptyMprcid == "" {
			if strings.Contains(payload, "CIF_REQST") {
				mrpcId = "CIF_REQST"
			} else {
				mrpcId = "OPS_REQST"
			}
		}
	}

	msg.content = common.NetString(buf)
	msg.IsRequest = isRequest
	msg.Direction = dir
	msg.origMsg = origMsgID
	msg.mrpcId = mrpcId
	msg.origSysID = origSysID
	msg.targSysID = targSysID
	msg.origPrcID = origPrcID
	msg.commandName = commandName
	msg.rsRetCode = rsRetCode
	t, err := timeParse(timePayload)
	if err != nil {
		//fmt.Printf("Error Parse date. %v\n", err)
		//msg.timePayload = t
	}
	msg.timePayload = t
	return msg, nil
}

// WriteToFile Запись в файл filename буффера buf
func WriteToFile(filename string, buf []byte) {

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	if _, err = file.Write(buf); err != nil {
		panic(err)
	}

	if err := file.Close(); err != nil {
		panic(err)
	}
}

// ExtractXML Извлекает данные из тегов xml
// field  тег, по которому нужно извлечь данные
// xmlText xml текст, откуда будут извлекаться данные
func ExtractXML(field, xmlText string) string {
	pattern := fmt.Sprintf("<%s.*?\\>(.*?)\\<.*?\\>", field)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return ""
	}
	findSub := re.FindStringSubmatch(xmlText)
	if findSub == nil {
		return ""
	}
	return findSub[1]
}

// ExtractInnerTagXML Извлекает данные из тегов xml
// tag  тег, по которому нужно извлечь данные, пример: <tag1="value"
// xmlText xml текст, откуда будут извлекаться данные
func ExtractInnerTagXML(tag, xmlText string) string {
	pattern := fmt.Sprintf("<%s=\"(.*?)\"", tag)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return ""
	}
	findSub := re.FindStringSubmatch(xmlText)
	if findSub == nil {
		return ""
	}
	return findSub[1]
}

func between(value string, a string, b string) string {
	// Get substring between two strings.
	posFirst := strings.Index(value, a)
	if posFirst == -1 {
		return ""
	}
	posLast := strings.Index(value, b)
	if posLast == -1 {
		return ""
	}
	posFirstAdjusted := posFirst + len(a)
	if posFirstAdjusted >= posLast {
		return ""
	}
	return value[posFirstAdjusted:posLast]
}

func timeParse(str string) (time.Time, error) {

	layout := "2006-01-02T15:04:05"
	if strings.Contains(str, "Z") {
		sizeZ := len(str[strings.LastIndex(str, ".")+1 : len(str)-1])
		layout = "2006-01-02T15:04:05." + strings.Repeat("0", sizeZ) + "Z"
	}
	if strings.Contains(str, "UTC") && strings.Contains(str, "+") {
		layout = "2006-01-02 06:19:35.1520974 +000 UTC"
	}
	if strings.Contains(str, "+") {
		endUTC := strings.LastIndex(str, "+")
		str = str[:endUTC] + "+00:00"
		layout = "2006-01-02T15:04:05Z07:00"
	}
	if strings.Contains(str, "/") && strings.Contains(str, ":") {
		layout = "01/02/2006 15:04:05"
	}
	if len(str) < 11 {
		layout = "2006-01-02"
	}

	t, err := time.Parse(layout, str)
	if err != nil {
		return t, err
	}
	return t, nil
}
