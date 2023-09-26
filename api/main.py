# Run with:
# For developing:    uvicorn main:app --reload
import os
from datetime import datetime
from datetime import timezone
from itertools import groupby

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
from brotli_asgi import BrotliMiddleware
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from covjson_pydantic.domain import Axes
from covjson_pydantic.domain import Domain
from covjson_pydantic.domain import DomainType
from covjson_pydantic.domain import ValuesAxis
from covjson_pydantic.ndarray import NdArray
from fastapi import FastAPI
from fastapi import Path
from fastapi import Query
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from geojson_pydantic import Point
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import AwareDatetime
from shapely import buffer
from shapely import geometry
from shapely import wkt


app = FastAPI()
app.add_middleware(BrotliMiddleware)


def collect_data(time_serie, observations):
    lat = time_serie.metadata.pos.lat
    lon = time_serie.metadata.pos.lon
    tuples = ((o.time.ToDatetime(tzinfo=timezone.utc), o.value) for o in observations.obs)
    (times, values) = zip(*tuples)
    param_id = time_serie.metadata.param_id

    return (lat, lon, times), param_id, values


def get_data_for_time_series(ts_response, grpc_stub):
    from_time = Timestamp()
    from_time.FromDatetime(datetime(2022, 12, 31))  # TODO: Get from request
    to_time = Timestamp()
    to_time.FromDatetime(datetime(2023, 11, 1))  # TODO: Get from request
    request = dstore.GetObsRequest(
        tsids=[ts.id for ts in ts_response.tseries],
        fromtime=from_time,
        totime=to_time,
    )
    response = grpc_stub.GetObservations(request)

    for i in range(0, len(ts_response.tseries)):
        assert response.tsobs[i].tsid == ts_response.tseries[i].id

    # Collect data
    coverages = []
    data = [
        collect_data(time_serie, observations)
        for (time_serie, observations) in zip(ts_response.tseries, response.tsobs)
    ]

    # Need to sort before using groupBy
    data.sort(key=lambda x: x[0])
    # The multiple coverage logic is not needed for this endpoint,
    # but we want to share this code between endpoints
    for (lat, lon, times), group in groupby(data, lambda x: x[0]):
        domain = Domain(
            domainType=DomainType.point_series,
            axes=Axes(
                x=ValuesAxis[float](values=[lon]),
                y=ValuesAxis[float](values=[lat]),
                t=ValuesAxis[AwareDatetime](values=times),
            ),
        )
        ranges = {
            param_id: NdArray(values=values, axisNames=["t", "y", "x"], shape=[len(values), 1, 1])
            for ((_, _, _), param_id, values) in group
        }

        coverages.append(Coverage(domain=domain, ranges=ranges))

    if len(coverages) == 1:
        return coverages[0]
    else:
        return CoverageCollection(coverages=coverages)


@app.get(
    "/collections/observations/locations",
    response_model=FeatureCollection,
    response_model_exclude_none=True,
)
def get_locations(
    bbox: str = Query(..., example="5.0,52.0,6.0,52.1")
) -> FeatureCollection:  # Hack to use string
    left, bottom, right, top = map(str.strip, bbox.split(","))
    poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    with grpc.insecure_channel(
        f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}"
    ) as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        ts_request = dstore.FindTSRequest(
            param_ids=["tn"],  # Hack
            inside=dstore.Polygon(
                points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]
            ),
        )
        ts_response = grpc_stub.FindTimeSeries(ts_request)

        features = [
            Feature(
                type="Feature",
                id=ts.metadata.station_id,
                properties=None,
                geometry=Point(
                    type="Point", coordinates=(ts.metadata.pos.lon, ts.metadata.pos.lat)
                ),
            )
            for ts in ts_response.tseries
        ]
        return FeatureCollection(features=features, type="FeatureCollection")


@app.get(
    "/collections/observations/locations/{location_id}",
    response_model=Coverage,
    response_model_exclude_none=True,
)
def get_data_location_id(
    location_id: str = Path(..., example="06260"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
):
    # TODO: There is no error handling of any kind at the moment!
    #  This is just a quick and dirty demo
    # TODO: Code does not handle nan when serialising to JSON
    with grpc.insecure_channel(
        f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}"
    ) as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        ts_request = dstore.FindTSRequest(
            station_ids=[location_id], param_ids=list(map(str.strip, parameter_name.split(",")))
        )
        ts_response = grpc_stub.FindTimeSeries(ts_request)
        return get_data_for_time_series(ts_response, grpc_stub)


@app.get(
    "/collections/observations/position",
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
def get_data_position(
    coords: str = Query(..., example="POINT(5.179705 52.0988218)"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
):
    point = wkt.loads(coords)
    assert point.geom_type == "Point"
    poly = buffer(point, 0.0001, quad_segs=1)  # Roughly 10 meters around the point
    return get_data_area(poly.wkt, parameter_name)


@app.get(
    "/collections/observations/area",
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
def get_data_area(
    coords: str = Query(..., example="POLYGON((5.0 52.0, 6.0 52.0,6.0 52.1,5.0 52.1, 5.0 52.0))"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
):
    poly = wkt.loads(coords)
    assert poly.geom_type == "Polygon"
    with grpc.insecure_channel(
        f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}"
    ) as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        ts_request = dstore.FindTSRequest(
            param_ids=list(map(str.strip, parameter_name.split(","))),
            inside=dstore.Polygon(
                points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]
            ),
        )
        ts_response = grpc_stub.FindTimeSeries(ts_request)
        return get_data_for_time_series(ts_response, grpc_stub)
