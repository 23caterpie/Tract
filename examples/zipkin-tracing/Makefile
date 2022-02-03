.PHONY: kafka-push
kafka-push:
	docker exec -it zipkin-tracing_kafka-local_1 kafka-console-producer.sh --broker-list localhost:9092 --topic requests

.PHONY: zipkin
zipkin:
	open http://localhost:9411/

.PHONY: metrics
metrics:
	open http://localhost:8081/metrics

.PHONY: pprof
pprof:
	open http://localhost:8081/debug/pprof/
