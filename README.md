# Hasura Oracle Connector

This repository contains the source code to the Oracle Connector for Hasura GraphQL Engine.

## Usage

### Prerequisites

- JDK 17
- Oracle Database 18c or later

### Building the Connector

To build the connector, run the following command:

```bash
./gradlew build
```

### Running the Connector

To run the connector in development mode, run the following command:

```bash
# Does not require that the connector has been built
./gradlew quarkusDev --console=plain
```

To run the connector in production mode, run the following command:

```bash
# Requires that the connector has been built
java -jar ./app/build/quarkus-app/quarkus-run.jar
```

### Configuration

Configuration variables can be set using environment variables or by editing the `application.properties` file.
The environment variable name is equivalent to the configuration variable name, but in uppercase and with `.` replaced by `_`.
For example, the environment variable for `quarkus.log.level` is `QUARKUS_LOG_LEVEL`.

Some common configuration variables are:
- `quarkus.log.level`: The log level for the connector. Defaults to `INFO`.
- `quarkus.http.port`: The port on which the connector will listen for requests. Defaults to `8081`.
- `hasura.agroal.connection_pool_configuration.reap_timeout`: The eviction timeout for idle connections in the connection pool. Defaults to `PT10M`.
- `hasura.agroal.connection_pool_configuration.max_lifetime`: The maximum lifetime of a connection in the connection pool. Defaults to `PT2H`.

For a full list of configuration variables, see the [Quarkus Configuration Reference](https://quarkus.io/guides/all-config).

### Connecting to the Connector

To connect to the connector, use the following URL when configuring the Data Connector in Hasura:

```
http(s)://<host>:<port>/api/v1/oracle
```

## Benchmarks

To give an idea of the performance of the connector, we have included some benchmarks.

These benchmarks were run under the following conditions:
- A local Oracle Database 21c instance running in a Docker container
- The JVM connector running on the host system
- The Hasura GraphQL Engine running in a Docker container

### Setup

The `graphql-bench` tool was used to run the benchmarks against the Chinook dataset.
Configuration for the benchmarks was as follows:

```yaml
url: http://localhost:8082/v1/graphql
queries:
  - name: ArtistsWithAlbums
    tools: [k6]
    execution_strategy: MAX_REQUESTS_IN_DURATION
    duration: 10s
    query: |
      query {
        ARTIST(limit: 100) {
          ARTISTID
          NAME
          ALBUMs {
            ARTISTID
            ALBUMID
            TITLE
          }
        }
      }
```

### Results

The above query tests making as many requests as possible in 10 seconds, fetching 100 artists and their albums each time.
The following results were obtained:

```
          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: /app/queries/bin/k6/loadScript.js
     output: json (/app/queries/src/executors/k6/tmp/k6_raw_stats.json)

  scenarios: (100.00%) 1 scenario, 10 max VUs, 40s max duration (incl. graceful stop):
           * ArtistsWithAlbums: 10 looping VUs for 10s (gracefulStop: 30s)


running (10.0s), 00/10 VUs, 2840 complete and 0 interrupted iterations
ArtistsWithAlbums ✓ [======================================] 10 VUs  10s

     ✓ is status 200
     ✓ no error in body

     checks.........................: 100.00% ✓ 5680       ✗ 0
     data_received..................: 43 MB   4.3 MB/s
     data_sent......................: 721 kB  72 kB/s
     http_req_blocked...............: avg=9.36µs   min=1.06µs  med=3.48µs  max=1.53ms   p(90)=5.81µs   p(95)=6.87µs
     http_req_connecting............: avg=737ns    min=0s      med=0s      max=285.51µs p(90)=0s       p(95)=0s
     http_req_duration..............: avg=34.9ms   min=16.74ms med=24.72ms max=665.31ms p(90)=33.17ms  p(95)=39.43ms
       { expected_response:true }...: avg=34.9ms   min=16.74ms med=24.72ms max=665.31ms p(90)=33.17ms  p(95)=39.43ms
     http_req_failed................: 0.00%   ✓ 0          ✗ 2840
     http_req_receiving.............: avg=158.18µs min=26.4µs  med=99.1µs  max=4.59ms   p(90)=202.15µs p(95)=273.55µs
     http_req_sending...............: avg=25.33µs  min=6.73µs  med=19.45µs max=2.38ms   p(90)=42.58µs  p(95)=49.05µs
     http_req_tls_handshaking.......: avg=0s       min=0s      med=0s      max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=34.71ms  min=16.61ms med=24.55ms max=665.11ms p(90)=33ms     p(95)=39.25ms
     http_reqs......................: 2840    283.481751/s
     iteration_duration.............: avg=35.24ms  min=17.06ms med=25.06ms max=667.17ms p(90)=33.52ms  p(95)=39.7ms
     iterations.....................: 2840    283.481751/s
     vus............................: 10      min=10       max=10
     vus_max........................: 10      min=10       max=10
```

We see a throughput of ~283 requests per second, with a p90 response time of ~33ms.

### Further Information

More granular performance information can be gathered by running Jaeger and connecting the Oracle connector to it.
If Jaeger is running locally, this will be done automatically.