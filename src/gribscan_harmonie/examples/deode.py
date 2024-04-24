# coding: utf-8

import datetime
from pathlib import Path

from gribscan_harmonie.load import create_loader


def get_files(t_analysis):
    subdir = "{t:%Y}/{t:%m}/{t:%d}/{t:%H}/".format(t=t_analysis)
    return list((fp_root / subdir).glob("GRIB*00s"))


get_files.dt_collection_analysis_timespan = None
get_files.dt_collection_analysis_interval = "PT24H"


if __name__ == "__main__":
    fp_root = Path("/scratch/snh/deode/CY46h1_HARMONIE_AROME_GAVLE_500m_v2/archive/")

    loader = create_loader(
        fn_source_files=get_files, fp_grib_indecies_root=Path("/tmp/lcd/")
    )

    t = datetime.datetime(year=2021, month=8, day=17)
    ds = loader(
        slice(t, t + datetime.timedelta(days=1)), level_type="heightAboveGround"
    )
    print(ds)
