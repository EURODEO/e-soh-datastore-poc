# E-SOH datastore variant 1: gRPC service written in Go

## Overview

This directory contains code that demonstrates how the E-SOH datastore could
be implemented as a [gRPC](https://grpc.io/) service written in
[Go](https://go.dev/).

Currently the datastore server is using a PostgreSQL server as its only
storage backend (for both metadata and observations).

**Note:** Unless otherwise noted, all commands described below should be run from the same
directory as this README file.

The code has been tested in the following environment:

### Service

|   |   |
|---|---|
| OS | [Ubuntu](https://ubuntu.com/) [22.04 Jammy](https://releases.ubuntu.com/jammy/) |
| [Docker](https://www.docker.com/) | 24.0.5 |
| [Docker Compose](https://www.docker.com/) | 2.20.2 |

### Python client example

|   |   |
|---|---|
| OS | Same as service |
| [Python](https://www.python.org/) | 3.11 |
| [grpcio-tools](https://grpc.io/docs/languages/python/quickstart/) | 1.56.2 |

## Some examples of using docker compose to manage the service

(**NOTE:** Any environment variables used on these examples are defined in a separate section below)

### Check current status

```text
docker compose ps -a
```

### Start service in normal mode (rolling buffer of observations within latest 24H)

```text
docker compose down --volumes
docker compose build
docker compose up -d
```

### Same as above, but with a safety margin of one minute in case 'current time' isn't 100% synchronized everywhere

```text
docker compose down --volumes
docker compose build
HITIME=-60 docker compose up -d
```

(note how we _subtract_ a _negative_ value to current time to get a value into the future)

### Start service in "infinite" mode (accommodating "all" possible obs times) and run a test

```text
docker compose down --volumes
docker compose --profile test build
DYNAMICTIME=false LOTIME=1000-01-01T00:00:00Z HITIME=9999-12-31T23:59:59Z docker compose up -d
DYNAMICTIME=false LOTIME=1000-01-01T00:00:00Z HITIME=9999-12-31T23:59:59Z docker compose run --rm loader
DYNAMICTIME=false LOTIME=1000-01-01T00:00:00Z HITIME=9999-12-31T23:59:59Z docker compose run --rm integration
```

**NOTE:** as an alternative to specifying environment variables explicitly on the command line (which can be quite verbose), they could instead be kept in a file called `.env`:

```text
DYNAMICTIME=false
LOTIME=1000-01-01T00:00:00Z
HITIME=9999-12-31T23:59:59Z
```

Using a `.env` file also makes it more practical to have all supported environment variables
explicitly defined and thus avoid warnings from `docker compose` due to undefined defaults
(defaults are defined in the Go code only). So for example, to get rid of the following warnings:

```text
$ docker compose up -d
WARN[0000] The "CLEANUPINTERVAL" variable is not set. Defaulting to a blank string.
WARN[0000] The "PUTOBSLIMIT" variable is not set. Defaulting to a blank string.
...
```

, simply ensure that `CLEANUPINTERVAL` and `PUTOBSLIMIT` are both defined in `.env`.

### Same as above, but specifying LOTIME and HITIME directly as seconds

First ensure `.env` has the following contents:

```text
DYNAMICTIME=false
LOTIME=-30610227208
HITIME=253402297199
```

Then run the same five docker compose commands as in the previous example (without specifying environment variables).

-------------

MORE DETAILS/EXAMPLES HERE!

## Collecting profiling stats from a running datastore service

**STEP 1:** Start the service (or ensure it already runs).

**STEP 2:** Collect profiling stats over a certain period, for example:

`go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=220`

(by default, the stats will be written to `~/pprof/`; see `go tool pprof --help` to see
all options)

**STEP 3:** Open a web page to visualize and inspect the results of a given profiling run,
for example:

`BROWSER=firefox go tool pprof -http=:8081 ~/pprof/pprof.dsserver.samples.cpu.001.pb.gz`

## Compiling datastore.proto and update go.sum to prevent IDEs from complaining

Whenever `datastore.proto` changes, it should be complied locally in order for
IDEs to recognize the current types and symbols.

```text
protoc --go_out=. --go-grpc_out=. protobuf/datastore.proto
```

Likewise, keeping `go.sum` up-to-date like this may also prevent certain
warnings/errors in IDEs:

```text
go mod tidy
```

## Environment variables

The following environment variables are supported:

Variable | Mandatory | Default value | Description
:--      | :--       | :--           | :--
`SERVERPORT`      | No  | `50050`            | Server port number.
`PGHOST`          | No  | `localhost`        | PostgreSQL host.
`PGPORT`          | No  | `5433`             | PostgreSQL port number.
`PGBUSER`         | No  | `postgres`         | PostgreSQL user name.
`PGPASSWORD`      | No  | `mysecretpassword` | PostgreSQL password.
`PGDBNAME`        | No  | `data`             | PostgreSQL database name.
`DYNAMICTIME`     | No  | `true`             | Whether the valid time range is _dynamic_ or _static_ (defined below).
`LOTIME`          | No  | `86400`            | The _earliest_ valid time as seconds to be either [1] subtracted from the current time (if the valid time range is _dynamic_) or [2] added to UNIX epoch (1970-01-01T00:00:00Z) (if the valid time range is _static_). In the case of a _static_ valid time range, the `LOTIME` can optionally be specified as an ISO-8601 datetime of the exact form `2023-10-10T00:00:00Z`.
`HITIME`          | No  | `-2`               | Same as `LOTIME`, but for the _latest_ valid time. Note a default leeway of 2 seconds into the future to reduce risk of missing the newest observations.
`CLEANUPINTERVAL` | No  | `86400`            | The minimum time duration in seconds between automatic cleanups (like removing obsolete observations from the physical store).
`PUTOBSLIMIT`     | No  | `100000`           | Maximum number of observations allowed in a single call to `PutObservations`.

**TODO:** Ensure that these variables are [passed properly](https://docs.docker.com/compose/environment-variables/set-environment-variables/) to the relevant `docker compose`
commands. Any secrets should be passed using a [special mechanism](https://docs.docker.com/compose/use-secrets/), etc.

## Testing the datastore service with gRPCurl

The datastore service can be tested with [gRPCurl](https://github.com/fullstorydev/grpcurl). Below are a few examples:

### List all services defined in the proto file

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto list
datastore.Datastore
```

### Describe all services defined in the proto file

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto describe
datastore.Datastore is a service:
service Datastore {
  rpc GetObservations ( .datastore.GetObsRequest ) returns ( .datastore.GetObsResponse );
  rpc PutObservations ( .datastore.PutObsRequest ) returns ( .datastore.PutObsResponse );
}
```

### Describe method PutObservations

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto describe datastore.Datastore.PutObservations
datastore.Datastore.PutObservations is a method:
rpc PutObservations ( .datastore.PutObsRequest ) returns ( .datastore.PutObsResponse );
```

### Describe message PutObsRequest

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto describe .datastore.PutObsRequest
datastore.PutObsRequest is a message:
message PutObsRequest {
  repeated .datastore.Metadata1 observations = 1;
}
```

### Insert observations

```text
$ grpcurl -d '{"observations": [{"ts_mdata": {"version": "version_dummy", "type": "type_dummy", "standard_name": "air_temperature", "unit": "celsius"}, "obs_mdata": {"id": "id_dummy", "geo_point": {"lat": 59.91, "lon": 10.75}, "pubtime": "2023-01-01T00:00:10Z", "data_id": "data_id_dummy", "obstime_instant": "2023-01-01T00:00:00Z", "value": "123.456"}}]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.PutObservations
...
```

### Retrieve all observations

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

### Retrieve observations in a time range

```text
$ grpcurl -d '{"interval": {"start": "2023-01-01T00:00:00Z", "end": "2023-01-01T00:00:10Z"}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

### Retrieve observations in a polygon

```text
$ grpcurl -d '{"inside": {"points": [{"lat": 59.90, "lon": 10.70}, {"lat": 59.90, "lon": 10.80}, {"lat": 60, "lon": 10.80}, {"lat": 60, "lon": 10.70}]}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

### Retrieve observations in both a time range and a polygon

```text
$ grpcurl -d '{"interval": {"start": "2023-01-01T00:00:00Z", "end": "2023-01-01T00:00:10Z"}, "inside": {"points": [{"lat": 59.90, "lon": 10.70}, {"lat": 59.90, "lon": 10.80}, {"lat": 60, "lon": 10.80}, {"lat": 60, "lon": 10.70}]}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

### Retrieve wind speed and air temperature observations in a time range and a polygon

```text
$ grpcurl -d '{"standard_names": ["wind_speed", "air_temperature"], "interval": {"start": "2023-01-01T00:00:00Z", "end": "2023-01-01T00:00:10Z"}, "inside": {"points": [{"lat": 59.90, "lon": 10.70}, {"lat": 59.90, "lon": 10.80}, {"lat": 60, "lon": 10.80}, {"lat": 60, "lon": 10.70}]}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

### List unique occurrences of time series metadata attribute 'standard_name'

```text
$ grpcurl -d '{"attrs": ["standard_name"]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetTSAttrGroups
{
  "groups": [
    {
      "combos": [
        {
          "standard_name": "air_pressure_at_sea_level"
        }
      ]
    },
    {
      "combos": [
        {
          "standard_name": "air_temperature"
        }
      ]
    },
...
```

### List unique combinations of time series metadata attributes 'platform' and 'standard_name'

```text
$ grpcurl -d '{"attrs": ["platform", "standard_name"]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetTSAttrGroups
{
  "groups": [
    {
      "combos": [
        {
          "platform": "06201",
          "standard_name": "air_pressure_at_sea_level"
        }
      ]
    },
    {
      "combos": [
        {
          "platform": "06201",
          "standard_name": "air_temperature"
        }
      ]
    },
...
```

### List unique occurrences of time series metadata attribute 'standard_name', and include associated instances

```text
$ grpcurl -d '{"attrs": ["standard_name"], "include_instances": true}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetTSAttrGroups
{
  "groups": [
    {
      "combos": [
        {
          "title": "Air Pressure at Sea Level 1 Min Average",
          "platform": "06208",
          "standard_name": "air_pressure_at_sea_level",
          "unit": "hPa",
          "instrument": "pp"
        },
        {
          "title": "Air Pressure at Sea Level 1 Min Average",
          "platform": "06348",
          "standard_name": "air_pressure_at_sea_level",
          "unit": "hPa",
          "instrument": "pp"
        },
...
```

### List unique combinations of time series metadata attributes 'platform' and 'standard_name', and include associated instances

```text
$ grpcurl -d '{"attrs": ["platform", "standard_name"], "include_instances": true}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetTSAttrGroups
{
  "groups": [
    {
      "combos": [
        {
          "title": "Air Temperature Minimum last 12 Hours",
          "platform": "06201",
          "standard_name": "air_temperature",
          "unit": "degrees Celsius",
          "instrument": "Tn12"
        },
        {
          "title": "Air Temperature Minimum last 14 Hours",
          "platform": "06201",
          "standard_name": "air_temperature",
          "unit": "degrees Celsius",
          "instrument": "Tn14"
        },
...
```

### Get the temporal- and spatial extent of all observations currently in the storage

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetExtents
{
  "timeExtent": {
    "start": "2022-12-31T00:00:00Z",
    "end": "2022-12-31T23:50:00Z"
  },
  "geoExtent": {
    "left": -68.2758333,
    "bottom": 12.13,
    "right": 7.1493220605216,
    "top": 55.399166666667
  }
}
```

## Testing the datastore service with a Python client

### Compiling the protobuf file

If necessary, compile the protobuf file first. The following command generates the files
`datastore_pb2.py` and `datastore_grpc.py` under `../examples/clients/python/`:

```text
python -m grpc_tools.protoc --proto_path=protobuf datastore.proto --python_out=../examples/clients/python --grpc_python_out=../examples/clients/python
```

### Running the client

The python client can be run like this:

```text
$ python ../examples/clients/python/client.py
response from callPutObs: status: -1
...
```

Testing the performance can be done with:

```bash
python -m cProfile -o <cprofile_output_file> <path_to_python_script>
```

Generate a dot graph / tree with:

```bash
gprof2dot --colour-nodes-by-selftime -f pstats <cprofile_output_file> | dot -Tpng -o <output_graph_file>
```
