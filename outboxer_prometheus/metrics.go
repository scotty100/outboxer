package outboxer_prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusMetricsAdapter struct {
	messageCreationVec, messageFind, messagePublish *prometheus.CounterVec
	publishDelayVec                                 *prometheus.HistogramVec
}

func NewPrometheusMetricsAdapter(reg *prometheus.Registry) *PrometheusMetricsAdapter {

	messageCreationInc := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_message_creation",
		Help: "outbox message creation metric",
	}, []string{"status", "topic", "messageType", "companyId", "outboxId"})

	messageFindInc := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_find",
		Help: "outbox find message metric",
	}, []string{"status"})

	messagePublishInc := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_publish",
		Help: "outbox message publish metric",
	}, []string{"status", "topic", "messageType", "companyId", "outboxId"})

	publishDelayDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "outbox_publish_delay",
		Help: "milliseconds delay between outbox message creation and publish",
	}, []string{"topic", "messageType", "companyId", "outboxId"})

	reg.MustRegister(messageCreationInc)
	reg.MustRegister(messageFindInc)
	reg.MustRegister(messagePublishInc)
	reg.MustRegister(publishDelayDur)

	return &PrometheusMetricsAdapter{
		messageCreationVec: messageCreationInc,
		messageFind:        messageFindInc,
		messagePublish:     messagePublishInc,
		publishDelayVec:    publishDelayDur,
	}
}

func (a *PrometheusMetricsAdapter) IncOutboxMessageCreation(status, topic, messageType, companyId string, outboxId int64) {
	a.messageCreationVec.WithLabelValues(status, topic, messageType, companyId, string(outboxId)).Inc()
}
func (a *PrometheusMetricsAdapter) IncOutboxFind(status string) {
	a.messageFind.WithLabelValues(status).Inc()
}

func (a *PrometheusMetricsAdapter) IncOutboxMessagePublish(status, topic, messageType, companyId string, outboxId int64) {
	a.messagePublish.WithLabelValues(status, topic, messageType, companyId, string(outboxId)).Inc()
}

func (a *PrometheusMetricsAdapter) OutboxPublishDelayMillis(milliseconds int64, topic, messageType, companyId string, outboxId int64) {
	a.publishDelayVec.WithLabelValues(topic, messageType, companyId, string(outboxId)).Observe(float64(milliseconds))
}
