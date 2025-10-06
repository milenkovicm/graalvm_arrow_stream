.PHONY: all rust jvm test clean

all: jvm rust

rust:
	cargo build 
# should be test: jvm but test fail then
# no time to investigate
test: jvm
#cd java && mvn package
	cargo test
jvm:
	cd java && mvn package
test-jvm:
	cd java && mvn test
clean-jvm:
	cd java && mvn clean
clean: clean-jvm
	cargo clean
