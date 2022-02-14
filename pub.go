package gtm

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/packetbeat/protos"
	"os"
)

// Transaction Publisher.
type transPub struct {
	sendRequest  bool
	sendResponse bool

	results protos.Reporter
}

func (pub *transPub) onTransaction(requ, resp *message) error {
	if pub.results == nil {
		return nil
	}
	if requ != nil && resp != nil {
		pub.results(pub.createEvent(requ, resp))
	}
	return nil
}

func (pub *transPub) createEvent(requ, resp *message) beat.Event {

	status := common.OK_STATUS
	// resp_time in milliseconds
	responseTime := int32(resp.Ts.Sub(requ.Ts).Nanoseconds() / 1e6)
	// костыль, если значение отрицательно, принудительно в 0
	if responseTime < 0 {
		//responseTime = -1*responseTime
		//responseTime = 0
		status = "Negative"
	}
	var responseTimePayload int32
	if resp.timePayload.IsZero() || requ.timePayload.IsZero() {
		responseTimePayload = 0
	} else {
		responseTimePayload = int32(resp.timePayload.Sub(requ.timePayload) / 1e6)
	}

	if responseTimePayload < 0 {
		responseTimePayload = 0
	}

	if responseTimePayload > 3600*1000 {
		responseTimePayload = 0
	}

	src := &common.Endpoint{
		IP:   requ.Tuple.SrcIP.String(),
		Port: requ.Tuple.SrcPort,
	}
	dst := &common.Endpoint{
		IP:   requ.Tuple.DstIP.String(),
		Port: requ.Tuple.DstPort,
	}

	if requ.origMsg != resp.origMsg {
		if len(requ.origMsg) > 0 && len(resp.origMsg) > 0 {
			status = common.ERROR_STATUS
		}

	}

	if responseTimePayload > responseTime {
		responseTimePayload = responseTime
	}

	fields := common.MapStr{
		"type":                "gtm",
		"status":              status,
		"responsetime":        responseTime,
		"responsetimepayload": responseTimePayload,
		"bytesRequest":        requ.Size,
		"bytesResponse":       resp.Size,
		"src":                 src,
		"dst":                 dst,
		"mrpcId":              requ.mrpcId,
		"origMsg":             requ.origMsg,
		"requestContent":      requ.content,
		"responseContent":     resp.content,
		"requestOrigSysID":    requ.origSysID,
		"requestTargSysID":    requ.targSysID,
		"responseOrigSysID":   resp.origSysID,
		"responseTargSysID":   resp.targSysID,
		"origPrcID":           requ.origPrcID,
		"commandName":         requ.commandName,
		"responseRetCode":     resp.rsRetCode,
		"deltaRequest":        responseTime - responseTimePayload,
	}
	// add processing notes/errors to event
	if len(requ.Notes)+len(resp.Notes) > 0 {
		fields["notes"] = append(requ.Notes, resp.Notes...)
	}

	//WriteRequestXMLToFileByPortFolder("/tmp/gtm/request", dst.Port, src.Port, requ)
	return beat.Event{
		Timestamp: requ.Ts,
		Fields:    fields,
	}
}

func WriteRequestXMLToFileByPortFolder(dir string, portDst, portSrc uint16,  requ *message) {

	directory := fmt.Sprintf("%s/%d_%d", dir, portDst, portSrc)
	filename := fmt.Sprintf("%s/%s.xml", directory, requ.origMsg)
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		_ = os.Mkdir(directory, 0755)
	}
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	if _, err = file.Write(requ.content); err != nil {
		panic(err)
	}

	if err := file.Close(); err != nil {
		panic(err)
	}
}
