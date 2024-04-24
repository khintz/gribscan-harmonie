import datetime
import warnings

import isodate

from .exceptions import InvalidArgument


def normalize_datetime_to_utc(t):
    """
    Put datetime in UTC timezone, if no timezone is given, assume local time
    """

    # check that t has timezone
    if t.tzinfo is None:
        # assume t is local time and issue warning
        # get local timezone (https://stackoverflow.com/a/39079819)
        tz_local = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
        warnings.warn(
            f"timestamp has no timezone, assuming system timezone ({tz_local})"
        )
        t = t.replace(tzinfo=tz_local)
    return t.astimezone(datetime.timezone.utc)


def normalise_duration(duration):
    if isinstance(duration, str):
        duration = isodate.parse_duration(duration)
    elif isinstance(duration, datetime.timedelta):
        pass
    else:
        raise InvalidArgument(
            f"Invalid duration argument: {duration}, expected ISO8601 duration string or datetime.timedelta"
        )
    return duration


def normalise_time_argument(time, allow_date=False):
    """
    Normalize time argument to `datetime.datetime` or `slice` of
    `datetime.datetime`'s (with optional step as `datetime.timedelta`). Arguments
    can be either `datetime.datetime`/`datetime.timedelta` or ISO8601 strings. If
    `allow_date==True`, ISO8601 dates and `datetime.date` are also allowed. All
    returned datetimes will be in UTC (if no timezone is given, local time
    will be assumed and times will be converted to UTC)
    """
    if isinstance(time, slice):
        if time.start is not None:
            time_start = normalise_time_argument(time.start, allow_date=allow_date)
        else:
            time_start = None

        if time.stop is not None:
            time_stop = normalise_time_argument(time.stop, allow_date=allow_date)
        else:
            time_stop = None

        if time.step is not None:
            if isinstance(time.step, str):
                time_step = isodate.parse_duration(time.step)
            elif isinstance(time.step, datetime.timedelta):
                time_step = time.step
            else:
                raise ValueError(
                    f"Invalid time step type: {type(time.step)}, expected str or datetime.timedelta"
                )
        else:
            time_step = None

        time = slice(time_start, time_stop, time_step)

    elif isinstance(time, str):
        try:
            time = normalize_datetime_to_utc(isodate.parse_datetime(time))
        except isodate.ISO8601Error as ex:
            if allow_date:
                time = isodate.parse_date(time)
            else:
                raise InvalidArgument(
                    f"Invalid time argument: {time}, expected ISO8601 datetime string"
                ) from ex

    elif isinstance(time, datetime.datetime):
        time = normalize_datetime_to_utc(time)

    elif isinstance(time, datetime.date) and allow_date:
        pass

    else:
        raise InvalidArgument(
            f"Invalid time argument: {time}, expected ISO8601 datetime string, datetime.datetime or slice"
        )

    return time
