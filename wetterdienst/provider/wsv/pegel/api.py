# -*- coding: utf-8 -*-
# Copyright (c) 2018-2021, earthobservations developers.
# Distributed under the MIT License. See LICENSE for more info.
from enum import Enum
from io import BytesIO

import pandas as pd

from wetterdienst import Provider, Kind, Resolution, Period
from wetterdienst.core.scalar.request import ScalarRequestCore
from wetterdienst.core.scalar.values import ScalarValuesCore
from wetterdienst.metadata.columns import Columns
from wetterdienst.metadata.datarange import DataRange
from wetterdienst.metadata.period import PeriodType
from wetterdienst.metadata.resolution import ResolutionType
from wetterdienst.metadata.timezone import Timezone
from wetterdienst.metadata.unit import OriginUnit, SIUnit
from wetterdienst.util.cache import CacheExpiry
from wetterdienst.util.network import NetworkFilesystemManager
from wetterdienst.util.parameter import DatasetTreeCore


class WsvPegelParameter(DatasetTreeCore):
    class ALL(Enum):
        WATER_LEVEL = "W"


class WsvPegelUnit(DatasetTreeCore):
    class ALL(Enum):
        WATER_LEVEL = OriginUnit.CENTIMETER.value, SIUnit.METER.value


class WsvPegelResolution(Enum):
    DYNAMIC = Resolution.DYNAMIC.value


class WsvPegelDataset(Enum):
    ALL = "ALL"


class WsvPegelValues(ScalarValuesCore):
    _string_parameters = ()
    _integer_parameters = ()
    _irregular_parameters = ()
    _date_parameters = ()

    _endpoint = "https://pegelonline.wsv.de/webservices/rest-api/v2/stations/{station_id}/{parameter}/measurements.json"
    _fs = NetworkFilesystemManager.get(CacheExpiry.NO_CACHE)

    @property
    def _data_tz(self) -> Timezone:
        return Timezone.GERMANY

    def _collect_station_parameter(self, station_id: str, parameter: Enum, dataset: Enum) -> pd.DataFrame:
        url = self._endpoint.format(station_id = station_id, parameter=parameter.value)

        try:
            response = self._fs.cat(url)
        except FileNotFoundError:
            return pd.DataFrame()

        df = pd.read_json(BytesIO(response))

        df = df.rename(columns={"timestamp": Columns.DATE.value, "value": Columns.VALUE.value})

        df[Columns.PARAMETER.value] = parameter.value.lower()

        return df


class WsvPegelRequest(ScalarRequestCore):
    _tz = Timezone.GERMANY

    _endpoint = "https://pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    _fs = NetworkFilesystemManager.get(CacheExpiry.ONE_HOUR)

    provider = Provider.WSV
    kind = Kind.OBSERVATION

    _resolution_type = ResolutionType.DYNAMIC
    _resolution_base = WsvPegelResolution

    _period_type = PeriodType.FIXED
    _period_base = Period.RECENT

    _data_range = DataRange.FIXED

    _has_datasets = False

    _parameter_base = WsvPegelParameter
    _dataset_base = WsvPegelDataset

    _unit_tree = WsvPegelUnit

    _has_tidy_data = True

    _values = WsvPegelValues
    
    def __init__(self, parameter):
        super(WsvPegelRequest, self).__init__(parameter=parameter, resolution=Resolution.DYNAMIC, period=Period.RECENT)

    def _all(self):
        response = self._fs.cat(self._endpoint)

        df = pd.read_json(BytesIO(response))

        df = df.rename(columns={"number": "station_id", "shortname": "name", "km": "river_kilometer"})

        df.water = df.water.map(lambda x: x["shortname"])

        return df


if __name__ == "__main__":
    stations = WsvPegelRequest(WsvPegelParameter.ALL.WATER_LEVEL).filter_by_station_id(126013)
    print(stations.df)

    values = next(stations.values.query())
    print(values.df)
