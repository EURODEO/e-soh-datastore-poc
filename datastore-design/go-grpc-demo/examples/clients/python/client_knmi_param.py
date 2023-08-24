#!/usr/bin/env python3
# tested with Python 3.11
import concurrent
import os
from pathlib import Path
from time import perf_counter

import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import xarray as xr
from dummy_data import param_ids

if __name__ == "__main__":
    total_time_start = perf_counter()
    data_paths = Path(Path(__file__).parents[5], "test-data", "KNMI").resolve().glob("*.nc")

    add_ts_request_messages = []
    put_observations_messages = []

    # TODO: The coords are not the same for every timeseries. There are 4 out of the 432 observations that have a
    #   different lat, lon and height. For the test data we use the first row. In the future we should look at
    #   iterating over the coords and if the 4 outliers are valid. This outliers can be found with:
    #   [np.array_equal(file["lat"].values[0], lats, equal_nan=True) for lats in file["lat"].values[1:]]
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        client = dstore_grpc.DatastoreStub(channel=channel)

        print("Collecting all the data...")
        collect_time_start = perf_counter()

        # TODO: How to deal with IDs. At the moment, I set them manually, but if the database or server could handle it,
        #   it would help when going for parallel processing when inserting. Do we want to use a UUID?
        ts_id = 1
        # with xr.open_mfdataset(paths=data_paths, combine="by_coords", engine="netcdf4", chunks=-1) as file:
        with xr.open_dataset(
            Path(Path(__file__).parents[5], "test-data", "KNMI", "20221231.nc"), engine="netcdf4", chunks=None  # disable dask
        ) as file:
            for param_id in param_ids:
                ts_observations = []

                param_file = file[param_id]
                for station_id, latitude, longitude, height in zip(
                    file["station"].values, file["lat"].values[0], file["lon"].values[0], file["height"].values[0]
                ):
                    tsMData = dstore.TSMetadata(
                        station_id=station_id,
                        param_id=param_id,
                        lat=latitude,
                        lon=longitude,
                        other1=param_file.name,
                        other2=param_file.long_name,
                        other3="value3",
                    )
                    request = dstore.AddTSRequest(
                        id=ts_id,
                        metadata=tsMData,
                    )
                    client.AddTimeSeries(request)

                    add_ts_request_messages.append(request)

                    station_slice = param_file.sel(station=station_id)
                    # TODO check if timestamp is correctly inserted
                    # observations = []
                    for time, obs_value in zip(
                        pd.to_datetime(station_slice["time"].data).to_pydatetime(), station_slice.data
                        # station_slice["time"].data.astype("datetime64[s]").astype("int64"), station_slice.data
                    ):
                        observations = []
                        ts = Timestamp()
                        ts.FromDatetime(time)
                        observations.append(
                            dstore.Observation(
                                time=ts,
                                value=obs_value,
                                metadata=dstore.ObsMetadata(
                                    field1="KNMI", field2="Royal Dutch Meteorological Institute"
                                ),
                            )
                        )
                        obs = [dstore.TSObservations(tsid=ts_id, obs=observations)]
                        request = dstore.PutObsRequest(tsobs=obs)
                        print(request)
                        client.PutObservations(request)

                    # ts_observations.append(dstore.TSObservations(tsid=ts_id, obs=observations))
                    ts_id += 1

                # request = dstore.PutObsRequest(tsobs=ts_observations)
                # put_observations_messages.append(request)

        print(f"Collected all data in {perf_counter() - collect_time_start} s")

        print("Add all the timeseries...")
        add_time_start = perf_counter()
        # for request in add_ts_request_messages:
        #     client.AddTimeSeries(request)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        #     results = list(executor.map(client.AddTimeSeries, add_ts_request_messages))
        print(f"Added all time series in {perf_counter() - add_time_start} s")

        print("Insert all the data...")
        insert_time_start = perf_counter()
        # for request in put_observations_messages:
        #     client.PutObservations(request)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        #     results = list(executor.map(client.PutObservations, put_observations_messages))
        print(f"Inserted all data in {perf_counter() - insert_time_start} s")

    print(f"Finished, total time elapsed: {perf_counter() - total_time_start} s")
