import datetime
import json
import multiprocessing as mp
import os
from functools import partial
from pathlib import Path
from typing import Union

import gribscan
import pandas as pd
import xarray as xr
from loguru import logger
from tqdm import tqdm

from .utils import normalise_time_argument


def _write_index(fp_grib, fp_grib_indecies_root=None):
    """
    Write a grib index file for fp_grib. If fp_grib_indecies_root is provided
    then the index file will be written in a path relative to that root.
    """
    fn_index = fp_grib.name + ".index.json"
    fp_index_parent = fp_grib.parent
    if fp_grib_indecies_root is not None:
        # strip the path root "/" so that we keep the same relative path inside FP_GRIB_INDECIES
        fp_index_parent = fp_grib_indecies_root / str(fp_index_parent)[1:]
    fp_index = fp_index_parent / fn_index
    if not fp_index.exists():
        fp_index.parent.mkdir(parents=True, exist_ok=True)
        gribscan.write_index(gribfile=fp_grib, idxfile=fp_index)
    return fp_index


def _write_zarr_indexes_for_grib_files(
    fps_grib,
    identifier,
    use_multiprocessing=True,
    fp_grib_indecies_root: Path = None,
):
    logger.debug(
        f"Opening the following GRIB files: {', '.join(str(fp) for fp in fps_grib)}"
    )

    logger.info(f"Writing index files for {len(fps_grib)} grib files")

    if not use_multiprocessing:
        fps_index = [
            _write_index(fp_grib, fp_grib_indecies_root=fp_grib_indecies_root)
            for fp_grib in tqdm(fps_grib)
        ]
    else:
        with mp.Pool() as pool:
            fps_index = list(
                tqdm(
                    pool.imap(
                        partial(
                            _write_index, fp_grib_indecies_root=fp_grib_indecies_root
                        ),
                        fps_grib,
                    ),
                    total=len(fps_grib),
                )
            )

    # find common path prefix between all fps_grib files
    common_prefix = os.path.commonpath([str(fp) for fp in fps_grib])
    if fp_grib_indecies_root is not None:
        common_prefix = str(fp_grib_indecies_root) + common_prefix
    # produce relative paths for the index files
    fps_index_relative = [fp.relative_to(common_prefix) for fp in fps_index]

    # build a zarr representation of the files into a single index
    refs = gribscan.grib_magic(
        filenames=fps_index_relative,
        magician=gribscan.magician.HarmonieMagician(),
        global_prefix=str(common_prefix) + "/",
    )

    fps_zarr_json = {}
    for level_type, ref in refs.items():
        fn = f"{level_type}.{identifier}.zarr.json"
        fp_zarr_json = fps_index[0].parent / fn
        with open(fp_zarr_json, "w") as f:
            json.dump(ref, f)

        logger.info(
            f"Built zarr index for analysis time {identifier} in {fp_zarr_json} for {level_type} level-type"
        )
        fps_zarr_json[level_type] = fp_zarr_json

    return fp_zarr_json


def create_loader(fn_source_files, fp_grib_indecies_root=None, **kwargs):
    def harmonie_loader(t_analysis: Union[datetime.datetime, slice], level_type: str):
        t_analysis = normalise_time_argument(t_analysis)

        index_collections = create_gribscan_indecies(
            t_analysis=t_analysis,
            fn_source_files=fn_source_files,
            fp_grib_indecies_root=fp_grib_indecies_root,
            **kwargs,
        )

        if not level_type in index_collections:
            raise ValueError(
                f"Level type {level_type} not found in parsed GRIB files. "
                f"The following level types are available: {', '.join(index_collections.keys())}"
            )

        fp_zarr_json = index_collections[level_type]

        ds = xr.open_zarr(f"reference::{fp_zarr_json}", consolidated=False)

        return ds

    return harmonie_loader


def create_gribscan_indecies(
    t_analysis: Union[datetime.datetime, slice],
    fn_source_files: callable,
    fp_grib_indecies_root: Path = None,
    **kwargs,
):
    """
    Write a grib index file for fp_grib. If fp_grib_indecies_root is provided
    then the index file will be written in a path relative to that root.
    """
    dt_collection_analysis_timespan = getattr(
        fn_source_files, "dt_collection_analysis_timespan"
    )

    t_analysis = normalise_time_argument(t_analysis)

    index_collections = {}

    fps_grib = []
    if isinstance(t_analysis, datetime.datetime):
        fps_grib = fn_source_files(t_analysis, **kwargs)
        # create identifier by iso8601 formatting the analysis time
        identifier = t_analysis.isoformat().replace(":", "").replace("-", "")
        fps_zarr_json = _write_zarr_indexes_for_grib_files(
            fps_grib,
            identifier=identifier,
            fp_grib_indecies_root=fp_grib_indecies_root,
        )
        index_collections[identifier] = fps_zarr_json
    elif isinstance(t_analysis, slice):
        assert (
            t_analysis.step is not None
        ), "Step must be provided to indicate analysis frequency"
        if dt_collection_analysis_timespan is None:
            # each collection of GRIB files comprises one analysis time
            ts_analysis = pd.date_range(
                t_analysis.start, t_analysis.stop, freq=t_analysis.step
            )
            for t_analysis in ts_analysis:
                fps_grib += fn_source_files(t_analysis, **kwargs)

        else:
            # each colletion of GRIB files comprises more than one analysis time
            pass

    return index_collections
