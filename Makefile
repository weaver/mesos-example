all:
	lein uberjar
	mv target/mesos-example-0.1.0-SNAPSHOT-standalone.jar playa-mesos
