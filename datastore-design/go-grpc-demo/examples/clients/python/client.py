#!/usr/bin/env python3
# tested with Python 3.11
import os
from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc


def callAddTimeSeries(stub):
    print('calling AddTimeSeries() ...')
    tsMData = dstore.TSMetadata(
        station_id='18700',
        param_id='211',
        lat=59.91,
        lon=10.75,
        other1='value1',
        other2='value2',
        other3='value3',
    )
    request = dstore.AddTSRequest(
        id=1234,
        metadata=tsMData,
    )
    response = stub.AddTimeSeries(request)
    print('    response: {}'.format(response))


def callPutObservations(stub):
    print('calling PutObservations() ...')
    obsMData = dstore.ObsMetadata(
        field1='value1',
        field2='value2',
    )
    timestamp = Timestamp()
    # TODO: Look into datetime now timestamp, the timestamps from the bulk loader look correct at the moment,
    #  but it looks as there are issues with a more precide datetime, thus instead of minute, nano second precision.
    timestamp.FromDatetime(datetime.now())
    obs = [
        dstore.Observation(
            time=timestamp,
            value=123.456,
            metadata=obsMData,
        )
    ]
    request = dstore.PutObsRequest(
        tsobs=[
            dstore.TSObservations(
                tsid=1234,
                obs=obs,
            )
        ],
    )
    response = stub.PutObservations(request)
    print('    response: {}'.format(response))


def callGetObservations(stub):
    print('calling GetObservations() ...')
    from_time = Timestamp()
    from_time.FromDatetime(datetime(2023, 1, 1))
    to_time = Timestamp()
    to_time.FromDatetime(datetime(2023, 10, 1))

    request = dstore.GetObsRequest(
        tsids=[1234, 5678, 9012],
        fromtime=from_time,
        totime=to_time,
    )
    response = stub.GetObservations(request)
    print('    response: {}'.format(response))


if __name__ == '__main__':

    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        stub = dstore_grpc.DatastoreStub(channel)

        callAddTimeSeries(stub)
        callPutObservations(stub)
        callGetObservations(stub)
