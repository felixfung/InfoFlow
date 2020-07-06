# a "vanilla" spark submit framework

.PHONY: run
default: build.timestamp

build.timestamp: $(wildcard src/main/scala/*.scala)
	sbt package
	touch build.timestamp

run: build.timestamp
	@echo spark-submit target/scala-2.11/infoflow_2.11-`/usr/bin/grep version build.sbt | cut -d = -f 2 | cut -d \" -f 2`.jar
	@spark-submit target/scala-2.11/infoflow_2.11-`/usr/bin/grep version build.sbt | cut -d = -f 2 | cut -d \" -f 2`.jar
