#!/usr/bin/env python3
# tested with Python 3.11
import os
from pathlib import Path
from time import perf_counter

from dummy_data import param_ids
import xarray as xr
import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc

if __name__ == '__main__':
    total_time_start = perf_counter()
    # data_paths = Path(Path(__file__).parents[5], "test-data", "KNMI").resolve().glob("*.nc")

    # TODO: The coords are not the same for every timeseries. There are 4 out of the 432 observations that have a
    #   different lat, lon and height. For the test data we use the first row. In the future we should look at
    #   iterating over the coords and if the 4 outliers are valid. This outliers can be found with:
    #   [np.array_equal(file["lat"].values[0], lats, equal_nan=True) for lats in file["lat"].values[1:]]
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        client = dstore_grpc.DatastoreStub(channel=channel)

        # TODO: How to deal with IDs. At the moment, I set them manually, but if the database or server could handle it,
        #   it would help when going for parallel processing when inserting. Do we want to use a UUID?
        ts_id = 1
        # with xr.open_mfdataset(paths=data_paths, combine="by_coords", engine="netcdf4", chunks=-1) as file:
        with xr.open_dataset(Path(Path(__file__).parents[5] / "test-data" / "KNMI" / "20221231.nc"), engine="netcdf4", chunks=-1) as file:
            for station_id, latitude, longitude, height in zip(
                    file["station"].values, file["lat"].values[0], file["lon"].values[0], file["height"].values[0]
            ):
                ts_observations = []
                station_slice = file.sel(station=station_id)

                for param_id in param_ids:
                    param_file = station_slice[param_id]
                    tsMData = dstore.TSMetadata(
                        station_id=station_id,
                        param_id=param_id,
                        lat=latitude,
                        lon=longitude,
                        other1=param_file.name,
                        other2=param_file.long_name,
                        other3='value3',
                    )
                    request = dstore.AddTSRequest(
                        id=ts_id,
                        metadata=tsMData,
                    )

                    client.AddTimeSeries(request)

                    # TODO check if timestamp is correctly inserted
                    observations = []
                    for time, obs_value in zip(param_file["time"].data.astype("datetime64[s]").astype("int64"), param_file.data):
                        observations.append(dstore.Observation(
                                        time=time,
                                        value=obs_value,
                                        metadata=dstore.ObsMetadata(
                                            field1="KNMI",
                                            field2="Royal Dutch Meteorological Institute"
                                        )
                                    ))

                    ts_observations.append(dstore.TSObservations(tsid=ts_id, obs=observations))
                    ts_id += 1

                print(f"Bulk inserting {len(ts_observations)} observations.")
                request = dstore.PutObsRequest(
                    tsobs=ts_observations
                )
                bulk_obs_insert_start = perf_counter()
                client.PutObservations(
                    request=request,
                )
                print(f"Finished bulk insert {perf_counter() - bulk_obs_insert_start}.")
    print(f"Finished, total time elapsed: {perf_counter() - total_time_start}")
