import json
from dataclasses import dataclass


@dataclass
class GreenTrip:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    passenger_count: int
    tip_amount: float


def _safe_int(val, default=0):
    import math
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return int(val)


def ride_from_row(row):
    return GreenTrip(
        PULocationID=_safe_int(row['PULocationID']),
        DOLocationID=_safe_int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=row['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        passenger_count=_safe_int(row['passenger_count']),
        tip_amount=float(row['tip_amount']),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return GreenTrip(**ride_dict)
