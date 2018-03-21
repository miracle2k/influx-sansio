"""HTTP-level Sans-IO helpers.
"""

from typing import Optional, Callable, Generator


def iter_resp(resp: dict, parser: Optional[Callable] = None) -> Generator:
    """Iterates a response JSON yielding data point by point.

    Can be used with both regular and chunked responses.

    By default, returns just a plain list of values representing each point,
    without column names, or other metadata.
    In case a specific format is needed, an optional ``parser`` argument can be passed.
    ``parser`` is a function that takes raw value list for each data point and a
    metadata dictionary containing all or a subset of the following:

    ``{'columns', 'name', 'tags', 'statement_id'}``.

    Sample parser function:

    .. code:: python
        def parser(x, meta):
            return dict(zip(meta['columns'], x))

    :param resp: Dictionary containing parsed JSON (output from InfluxDBClient.query)
    :param parser: Optional parser function
    """
    for statement in resp['results']:
        if 'series' not in statement:
            continue
        for series in statement['series']:
            meta = {k: series[k] for k in series if k != 'values'}
            meta['statement_id'] = statement['statement_id']
            for point in series['values']:
                if parser is None:
                    yield point
                else:
                    yield parser(point, meta)


def check_error(response):
    """Checks for JSON error messages and raises Python exception
    """
    if 'error' in response:
        raise InfluxDBError(response['error'])
    elif 'results' in response:
        for statement in response['results']:
            if 'error' in statement:
                msg = '{d[error]} (statement {d[statement_id]})'
                raise InfluxDBError(msg.format(d=statement))