# -*- coding: utf-8 -*-
# Copyright (c) 2018-2021, earthobservations developers.
# Distributed under the MIT License. See LICENSE for more info.
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import pytz
from pandas._libs.tslibs.timestamps import Timestamp
from pandas._testing import assert_frame_equal

from wetterdienst.provider.dwd.observation import (
    DwdObservationDataset,
    DwdObservationPeriod,
    DwdObservationRequest,
    DwdObservationResolution,
)
from wetterdienst.util.geo import Coordinates, derive_nearest_neighbours

EXPECTED_STATIONS_DF = pd.DataFrame(
    {
        "station_id": pd.Series(["02480", "04411", "07341"], dtype=str),
        "from_date": [
            Timestamp("2004-09-01 00:00:00", tzinfo=pytz.UTC),
            Timestamp("2002-01-24 00:00:00", tzinfo=pytz.UTC),
            Timestamp("2005-07-16 00:00:00", tzinfo=pytz.UTC),
        ],
        "height": pd.Series(
            [
                108.0,
                155.0,
                119.0,
            ],
            dtype=float,
        ),
        "latitude": pd.Series(
            [
                50.0643,
                49.9195,
                50.0899,
            ],
            dtype=float,
        ),
        "longitude": pd.Series(
            [
                8.993,
                8.9671,
                8.7862,
            ],
            dtype=float,
        ),
        "name": pd.Series(
            [
                "Kahl/Main",
                "Schaafheim-Schlierbach",
                "Offenbach-Wetterpark",
            ],
            dtype=str,
        ),
        "state": pd.Series(
            [
                "Bayern",
                "Hessen",
                "Hessen",
            ],
            dtype=str,
        ),
        "distance": pd.Series(
            [
                9.759384982994229,
                10.156943448624304,
                12.882693521631097,
            ],
            dtype=float,
        ),
    }
)


@pytest.mark.remote
def test_dwd_observation_stations_nearby_number_single():

    # Test for one nearest station
    request = DwdObservationRequest(
        DwdObservationDataset.TEMPERATURE_AIR,
        DwdObservationResolution.HOURLY,
        DwdObservationPeriod.RECENT,
        datetime(2020, 1, 1),
        datetime(2020, 1, 20),
    )

    nearby_station = request.filter_by_rank(
        50.0,
        8.9,
        1,
    )
    nearby_station = nearby_station.df.drop("to_date", axis="columns")

    assert_frame_equal(nearby_station, EXPECTED_STATIONS_DF.iloc[[0], :])


@pytest.mark.remote
def test_dwd_observation_stations_nearby_number_multiple():
    request = DwdObservationRequest(
        DwdObservationDataset.TEMPERATURE_AIR,
        DwdObservationResolution.HOURLY,
        DwdObservationPeriod.RECENT,
        datetime(2020, 1, 1),
        datetime(2020, 1, 20),
    )
    nearby_station = request.filter_by_rank(
        50.0,
        8.9,
        3,
    )
    nearby_station = nearby_station.df.drop("to_date", axis="columns")

    assert_frame_equal(nearby_station, EXPECTED_STATIONS_DF)


@pytest.mark.remote
def test_dwd_observation_stations_nearby_distance():
    request = DwdObservationRequest(
        DwdObservationDataset.TEMPERATURE_AIR,
        DwdObservationResolution.HOURLY,
        DwdObservationPeriod.RECENT,
        datetime(2020, 1, 1),
        datetime(2020, 1, 20),
    )
    # Kilometers
    nearby_station = request.filter_by_distance(50.0, 8.9, 16.13, "km")
    nearby_station = nearby_station.df.drop("to_date", axis="columns")

    assert_frame_equal(nearby_station, EXPECTED_STATIONS_DF)

    # Miles
    nearby_station = request.filter_by_distance(50.0, 8.9, 10.03, "mi")
    nearby_station = nearby_station.df.drop(columns="to_date")

    assert_frame_equal(nearby_station, EXPECTED_STATIONS_DF)


@pytest.mark.remote
def test_dwd_observation_stations_bbox():
    request = DwdObservationRequest(
        DwdObservationDataset.TEMPERATURE_AIR,
        DwdObservationResolution.HOURLY,
        DwdObservationPeriod.RECENT,
        datetime(2020, 1, 1),
        datetime(2020, 1, 20),
    )
    nearby_station = request.filter_by_bbox(left=8.7862, bottom=49.9195, right=8.993, top=50.0899)
    nearby_station = nearby_station.df.drop("to_date", axis="columns")

    assert_frame_equal(nearby_station, EXPECTED_STATIONS_DF.drop(columns=["distance"]))


@pytest.mark.remote
def test_dwd_observation_stations_empty():
    request = DwdObservationRequest(
        DwdObservationDataset.TEMPERATURE_AIR,
        DwdObservationResolution.HOURLY,
        DwdObservationPeriod.RECENT,
        datetime(2020, 1, 1),
        datetime(2020, 1, 20),
    )

    # Bbox
    assert request.filter_by_bbox(
        left=-100,
        bottom=-20,
        right=-90,
        top=-10,
    ).df.empty


@pytest.mark.remote
def test_dwd_observation_stations_fail():
    # Number
    with pytest.raises(ValueError):
        DwdObservationRequest(
            DwdObservationDataset.TEMPERATURE_AIR,
            DwdObservationResolution.HOURLY,
            DwdObservationPeriod.RECENT,
            datetime(2020, 1, 1),
            datetime(2020, 1, 20),
        ).filter_by_rank(
            51.4,
            9.3,
            0,
        )
    # Distance
    with pytest.raises(ValueError):
        DwdObservationRequest(
            DwdObservationDataset.TEMPERATURE_AIR,
            DwdObservationResolution.HOURLY,
            DwdObservationPeriod.RECENT,
            datetime(2020, 1, 1),
            datetime(2020, 1, 20),
        ).filter_by_distance(
            51.4,
            9.3,
            -1,
        )
    # Bbox
    with pytest.raises(ValueError):
        DwdObservationRequest(
            DwdObservationDataset.TEMPERATURE_AIR,
            DwdObservationResolution.HOURLY,
            DwdObservationPeriod.RECENT,
            datetime(2020, 1, 1),
            datetime(2020, 1, 20),
        ).filter_by_bbox(left=10, bottom=10, right=5, top=5)


def test_derive_nearest_neighbours():
    coords = Coordinates(np.array([50.0, 51.4]), np.array([8.9, 9.3]))

    metadata = pd.DataFrame(
        {
            "station_id": [4371, 4373, 4411, 13904, 13965, 15207],
            "latitude": [52.1042, 52.8568, 49.9195, 55.0, 48.2639, 51.2835],
            "longitude": [8.7521, 11.1319, 8.9671, 6.3333, 8.8134, 9.359],
        }
    )

    distances, indices_nearest_neighbours = derive_nearest_neighbours(
        latitudes=metadata["latitude"].values,
        longitudes=metadata["longitude"].values,
        coordinates=coords,
        number_nearby=1,
    )

    np.testing.assert_array_almost_equal(distances, np.array([[0.001594], [0.002133]]))

    np.testing.assert_array_almost_equal(indices_nearest_neighbours, np.array([[2], [5]]))
