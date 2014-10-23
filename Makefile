all: build

build:
	lein uberjar

deploy:
	mv target/mesos-example-0.1.0-SNAPSHOT-standalone.jar playa-mesos

run: build deploy
	cd playa-mesos && vagrant ssh -c 'java -Djava.library.path=/usr/local/lib -jar /vagrant/mesos-example-0.1.0-SNAPSHOT-standalone.jar cluster'
