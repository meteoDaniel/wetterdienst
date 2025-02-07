# -*- coding: utf-8 -*-
# Copyright (c) 2018-2021, earthobservations developers.
# Distributed under the MIT License. See LICENSE for more info.
from datetime import datetime

import pytest

from wetterdienst.exceptions import InvalidEnumeration
from wetterdienst.provider.dwd.radar import DwdRadarParameter, DwdRadarValues
from wetterdienst.provider.dwd.radar.metadata import (
    DwdRadarDataFormat,
    DwdRadarDate,
    DwdRadarPeriod,
    DwdRadarResolution,
)
from wetterdienst.provider.dwd.radar.sites import DwdRadarSite


def test_radar_request_site_historic_pe_wrong_parameters():
    """
    Verify acquisition of radar/site/PE_ECHO_TOP data croaks
    when omitting RadarDataFormat.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DwdRadarValues(
            parameter=DwdRadarParameter.PE_ECHO_TOP,
            site=DwdRadarSite.BOO,
            start_date=datetime.utcnow(),
        )
        next(request.query())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'format' is missing")


def test_radar_request_site_historic_pe_future(caplog):
    """
    Verify that ``DWDRadarRequest`` will properly emit
    log messages when hitting empty results.

    This time for PE_ECHO_TOP data.
    """

    request = DwdRadarValues(
        parameter=DwdRadarParameter.PE_ECHO_TOP,
        site=DwdRadarSite.BOO,
        fmt=DwdRadarDataFormat.BUFR,
        start_date="2099-01-01 00:00:00",
    )
    results = list(request.query())
    assert results == []

    assert "WARNING" in caplog.text
    assert "No radar file found" in caplog.text


def test_radar_request_site_latest_sweep_pcp_v_hdf5():
    """
    Verify requesting latest HDF5 data croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DwdRadarValues(
            parameter=DwdRadarParameter.SWEEP_PCP_VELOCITY_H,
            site=DwdRadarSite.BOO,
            fmt=DwdRadarDataFormat.HDF5,
            start_date=DwdRadarDate.LATEST,
        )

        list(request.query())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("HDF5 data has no '-latest-' files")


def test_radar_request_site_latest_sweep_pcp_v_hdf5_wrong_parameters():
    """
    Verify requesting HDF5 data without RadarDataFormat croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DwdRadarValues(
            parameter=DwdRadarParameter.SWEEP_PCP_VELOCITY_H,
            site=DwdRadarSite.BOO,
            start_date=DwdRadarDate.CURRENT,
        )

        list(request.query())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'format' is missing")


def test_radar_request_site_without_site():
    """
    Verify requesting site data without site croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DwdRadarValues(
            parameter=DwdRadarParameter.SWEEP_PCP_VELOCITY_H,
            start_date=DwdRadarDate.LATEST,
        )

        list(request.query())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'site' is missing")


def test_radar_request_hdf5_without_subset():
    """
    Verify requesting HDF5 data without "subset" croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DwdRadarValues(
            parameter=DwdRadarParameter.SWEEP_PCP_VELOCITY_H,
            site=DwdRadarSite.BOO,
            fmt=DwdRadarDataFormat.HDF5,
            start_date=DwdRadarDate.MOST_RECENT,
        )

        list(request.query())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'subset' is missing")


@pytest.mark.remote
@pytest.mark.parametrize(
    "time_resolution",
    [
        DwdRadarResolution.DAILY,
        DwdRadarResolution.HOURLY,
    ],
)
def test_radar_request_radolan_cdc_latest(time_resolution):
    """
    Verify requesting latest RADOLAN_CDC croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DwdRadarValues(
            parameter=DwdRadarParameter.RADOLAN_CDC,
            resolution=time_resolution,
            start_date=DwdRadarDate.LATEST,
        )

        list(request.query())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("RADOLAN_CDC data has no '-latest-' files")


def test_radar_request_radolan_cdc_invalid_time_resolution():
    """
    Verify requesting 1-minute RADOLAN_CDC croaks.
    """

    with pytest.raises(InvalidEnumeration):
        DwdRadarValues(
            parameter=DwdRadarParameter.RADOLAN_CDC,
            resolution="minute_1",
            period=DwdRadarPeriod.RECENT,
            start_date="2019-08-08 00:50:00",
        )


@pytest.mark.remote
def test_radar_request_radolan_cdc_future(caplog):
    """
    Verify that ``DWDRadarRequest`` will properly emit
    log messages when hitting empty results.

    This time for RADOLAN_CDC data.
    """
    request = DwdRadarValues(
        parameter=DwdRadarParameter.RADOLAN_CDC,
        resolution="daily",
        period=DwdRadarPeriod.RECENT,
        start_date="2099-01-01 00:50:00",
    )

    results = list(request.query())
    assert results == []

    assert "WARNING" in caplog.text
    assert "No radar file found" in caplog.text
