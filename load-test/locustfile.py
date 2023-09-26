# Use the following command to generate the python protobuf stuff in the correct place (from the root of the repository)  # noqa: E501
# python -m grpc_tools.protoc --proto_path=datastore/protobuf datastore.proto --python_out=load-test --grpc_python_out=load-test  # noqa: E501
import random
from datetime import datetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc_user
from google.protobuf.timestamp_pb2 import Timestamp
from locust import task


class StoreGrpcUser(grpc_user.GrpcUser):
    host = "localhost:50050"
    stub_class = dstore_grpc.DatastoreStub

    @task
    def find_debilt_humidity(self):
        ts_request = dstore.FindTSRequest(station_ids=["06260"], param_ids=["rh"])
        ts_response = self.stub.FindTimeSeries(ts_request)
        assert len(ts_response.tseries) == 1

    @task
    def get_data_random_timeserie(self):
        ts_id = random.randint(1, 55 * 44)

        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 11, 1))
        request = dstore.GetObsRequest(
            tsids=[ts_id],
            fromtime=from_time,
            totime=to_time,
        )
        response = self.stub.GetObservations(request)
        assert len(response.tsobs[0].obs) == 144
