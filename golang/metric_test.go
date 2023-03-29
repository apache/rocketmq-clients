package golang

import (
	"testing"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// This test is designed to verify there is no data race in dcmp.Reset
func TestDefaultClientMeterProviderResetNoDataRace(t *testing.T) {
	cli := BuildCLient(t)
	metric := &v2.Metric{On: false, Endpoints: cli.accessPoint}

	for i := 0; i < 5; i++ {
		go func() {
			cli.clientMeterProvider.Reset(metric)
		}()
	}
}
