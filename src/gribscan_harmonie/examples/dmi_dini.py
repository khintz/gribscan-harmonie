import datetime
import tempfile
from pathlib import Path

import isodate
from loguru import logger

from ..load import create_loader

# /dmidata/cache/mdcdev/gdb/grib2/dini/sf/2023102800/001
FILEPATH_ROOT = "/dmidata/cache/mdc{partition}/gdb/grib2/dini/"
FILEPATH_SUFFIX_FORMAT = "{data_kind}/{analysis_time:%Y%m%d%H}"
FILENAME_FORMAT = "{forecast_hour:03d}"
FORECAST_RESOLUTION = datetime.timedelta(hours=1)
FORECAST_DURATION = datetime.timedelta(hours=54)
FORECAST_PRODUCTION_START = isodate.parse_datetime("2024-03-19T00:00Z")


def find_dini_grib_files_collection(
    analysis_time: datetime.datetime, forecast_duration: datetime.timedelta = None
):
    assert isinstance(analysis_time, datetime.datetime)

    if analysis_time >= FORECAST_PRODUCTION_START:
        partition = "prd"
    else:
        partition = "dev"

    fp_forecast_root = Path(
        FILEPATH_ROOT.format(partition=partition)
    ) / FILEPATH_SUFFIX_FORMAT.format(analysis_time=analysis_time, data_kind="sf")

    if forecast_duration is None:
        forecast_duration = FORECAST_DURATION

    n_output_steps = forecast_duration // FORECAST_RESOLUTION

    fps_grib = [
        fp_forecast_root / FILENAME_FORMAT.format(forecast_hour=forecast_hour)
        for forecast_hour in range(n_output_steps)
    ]

    return fps_grib


find_dini_grib_files_collection.dt_collection_analysis_timespan = None


if __name__ == "__main__":
    # DINI forecast runs every 3 hours starting at 00:00 UTC
    # find time of most recent forecast that is at least 6 hours old, i.e. round to nearest six horus
    t_analysis = (datetime.datetime.utcnow() - datetime.timedelta(hours=6)).replace(
        minute=0, second=0, microsecond=0
    )
    t_analysis = t_analysis - datetime.timedelta(hours=t_analysis.hour % 6)

    tempdir = tempfile.TemporaryDirectory()

    harmonie_loader = create_loader(
        fn_source_files=find_dini_grib_files_collection,
        fp_grib_indecies_root=Path(tempdir.name),
    )

    ds = harmonie_loader(t_analysis=t_analysis, level_type="isobaricInhPa")

    logger.info(ds)

    ds = harmonie_loader(
        t_analysis=slice(t_analysis, t_analysis + datetime.timedelta(hours=3), "PT1H"),
        level_type="heightAboveGround",
    )

    logger.info(ds)
