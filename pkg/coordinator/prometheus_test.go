package coordinator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusMetricsUseGraphScopedRecordUnits(t *testing.T) {
	w := httptest.NewRecorder()
	writePrometheus(w, "graph-a", Metrics{
		Channels: []ChannelMetrics{{
			Name: "input", Produced: 13, Acknowledged: 8, Pending: 3,
			InFlight: 2, Lost: 5, Overflowed: 1, Epoch: 4,
		}},
		Operations: []OperationMetrics{{Name: "worker", RunnableTasks: 2, LivePods: 3}},
	})
	body := w.Body.String()
	for _, want := range []string{
		`stark8s_channel_acknowledged_records_total{graph="graph-a",channel="input"} 8`,
		`stark8s_channel_lost_records_total{graph="graph-a",channel="input"} 5`,
		`stark8s_channel_committed_epoch{graph="graph-a",channel="input"} 4`,
		`stark8s_operation_live_pods{graph="graph-a",operation="worker"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output does not contain %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "segment_id") || strings.Contains(body, "record_id") {
		t.Fatalf("metrics contain high-cardinality labels:\n%s", body)
	}
}

func TestPrometheusPathIsSeparateFromJSONMetrics(t *testing.T) {
	server := httptest.NewServer(HandlerForGraph(New("self:8090"), "graph-a"))
	defer server.Close()
	prometheus, err := http.Get(server.URL + PathPrometheusMetrics)
	if err != nil {
		t.Fatal(err)
	}
	defer prometheus.Body.Close()
	if got := prometheus.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("Prometheus content type %q", got)
	}
	jsonMetrics, err := http.Get(server.URL + PathMetrics)
	if err != nil {
		t.Fatal(err)
	}
	defer jsonMetrics.Body.Close()
	if got := jsonMetrics.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("JSON metrics content type %q", got)
	}
}
