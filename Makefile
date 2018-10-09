UBERJAR = target/uberjar/party-bus-0.1.0-SNAPSHOT-standalone.jar

LOCAL_ARGS = --config resources/config.edn \
             --listen-address 127.0.0.1:12080 \
             -c 127.0.0.1:12080 \
             -d 127.0.0.2 -d 127.0.0.3 -d 127.0.0.4 \
             -N 127.0.0.1:12100 -N 127.0.0.1:12101 -N 127.0.0.1:12102 \
             -N 127.0.0.2:12100 -N 127.0.0.2:12101

build:
	lein do clean, cljsbuild once min, uberjar

run-local:
	java -jar $(UBERJAR) $(LOCAL_ARGS)

run-dev:
	lein run $(LOCAL_ARGS)

