import trio
import asks
from asks import BasicAuth

from . import abc


class InfluxDBClient(abc.InfluxDBClient):
    """
    `asks`-based implementation.

    Args:
        session: HTTP session
    """
    def __init__(self, *, session=None, **kwargs):
        self._session = kwargs.pop('session', asks)

        username = kwargs.get('username')
        password = kwargs.get('password')
        if username or password:
            self.auth = BasicAuth((username, password))
        else:
            self.auth = None

        super().__init__(**kwargs)

    async def _request(self, method, url, headers=None, data=None, stream=False):
        # asks seems to have a bug and will stream even if stream=False
        response = await self._session.request(
            method, url, headers=headers, data=data, stream=stream or None, auth=self.auth)
        return response.status_code, response.headers, response.body if stream else response.content