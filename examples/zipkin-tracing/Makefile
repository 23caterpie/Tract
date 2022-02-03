# Opens a command line producers into kafka.
# type a number and hit enter to send it to the app to be square rooted.
.PHONY: kafka-push
kafka-push:
	docker exec -it zipkin-tracing_kafka-local_1 kafka-console-producer.sh --broker-list localhost:9092 --topic requests

# Opens the Zipkin UI to viewing live traces.
.PHONY: zipkin
zipkin:
	open http://localhost:9411/

# Views raw metrics from the app.
.PHONY: metrics
metrics:
	open http://localhost:8081/metrics

# Views live profiling of the app.
.PHONY: pprof
pprof:
	open http://localhost:8081/debug/pprof/

# Opens the Prometheus UI for viewing live metrics.
.PHONY: prom
prom:
	open http://localhost:9090/

# Opens the Prometheus UI to an example metric.
.PHONY: prom-example
prom-example:
	open "http://localhost:9090/graph?g0.expr=%20%20rate(zipkin_tracing_example_octract_worker_input_latency_sum%5B5m%5D)%0A%2F%0A%20%20rate(zipkin_tracing_example_octract_worker_input_latency_count%5B5m%5D)&g0.tab=1&g0.stacked=0&g0.show_exemplars=0&g0.range_input=5m"
