.PHONY: kafka-push
kafka-push:
	docker exec -it zipkin-tracing_kafka-local_1 kafka-console-producer.sh --broker-list localhost:9092 --topic requests
