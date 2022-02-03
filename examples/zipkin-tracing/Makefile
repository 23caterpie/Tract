NULL :=
TAB := $(NULL)	$(NULL)

# A good starting point.
.PHONY: all
all: up raw-metrics pprof prom-example zipkin
	$(info **********************************)
	$(info To send messages to the app:)
	$(info $(TAB)make kafka-push)
	$(info To stop the app use:)
	$(info $(TAB)make down)

# Targets useful to run the example app locally using docker compose.
.PHONY: up
.PHONY: down
.PHONY: logs
# Starts the app.
up:
	docker compose up -d
# Stops the app.
down:
	docker compose down --volumes
# Views the logs of the app.
logs:
	docker compose logs -f zipkin-tracing-example

# Targets that are useful to use when the example app is running.
.PHONY: kafka-push
.PHONY: zipkin
.PHONY: raw-metrics
.PHONY: pprof
.PHONY: prom
.PHONY: prom-example

# Opens a command line producers into kafka.
# type a number and hit enter to send it to the app to be square rooted.
kafka-push:
	docker exec -it zipkin-tracing-kafka-local-1 kafka-console-producer.sh --broker-list localhost:9092 --topic requests

# Opens the Zipkin UI to viewing live traces.
zipkin:
	open http://localhost:9411/

# Views raw metrics from the app.
raw-metrics:
	open http://localhost:8081/metrics

# Views live profiling of the app.
pprof:
	open http://localhost:8081/debug/pprof/

# Opens the Prometheus UI for viewing live metrics.
prom:
	open http://localhost:9090/

# Opens the Prometheus UI to an example metric.
prom-example:
	open "http://localhost:9090/graph?g0.expr=%20%20rate(zipkin_tracing_example_octract_worker_work_latency_sum%5B5m%5D)%0A%2F%0A%20%20rate(zipkin_tracing_example_octract_worker_work_latency_count%5B5m%5D)&g0.tab=1&g0.stacked=0&g0.show_exemplars=0&g0.range_input=5m"
