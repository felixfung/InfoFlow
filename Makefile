# a "vanilla" spark submit framework

.PHONY: run
default: build.timestamp

build.timestamp: $(wildcard src/main/scala/*.scala)
	sbt package
	touch build.timestamp

run: build.timestamp
	spark-submit target/scala-2.11/infoflow_2.11-1.0.jar
