"""Utility to retry HTTP requests with exponential backoff interval
"""
import asyncio
import logging
from typing import Any, Callable, List

import aiohttp
from aiohttp import ClientResponse

logger = logging.getLogger(__name__)


HTTP_STATUS_CODES_TO_RETRY = [500, 502, 503, 504]


class FailedRequest(Exception):
    """
    A wrapper for all possible exceptions during an HTTP request

    Args:
        raised (str): Exception type
        message (str): Exception message
        code (str): HTTP status code
        url (str): URL that was requested
    """

    code = 0
    message = ""
    url = ""
    raised = ""

    def __init__(
        self,
        *,
        raised: str = "",
        message: str = "",
        code: str = "",
        url: str = "",
    ):
        self.raised = raised
        self.message = message
        self.code = code
        self.url = url

        super().__init__(
            "code={c} message={m} raised={r} url={u}".format(
                c=self.code, m=self.message, r=self.raised, u=self.url
            )
        )


async def send_http(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    retries: int = 1,
    interval: int = 1,
    backoff: int = 2,
    http_status_codes_to_retry: List[int] = HTTP_STATUS_CODES_TO_RETRY,
    fn: Callable[[ClientResponse], Any] = lambda x: x,
    **kwargs: Any,
):
    """
    Send an HTTP request and implement retry logic

    Arguments:
        session (aiohttp.ClientSession): A client aiohttp session object
        method (str): Method to use eg. "get"
        url (str): URL for the request
        retries (int): Number of times to retry in case of failure
        interval (float): Time to wait before retries
        backoff (int): Multiply interval by this factor after each failure
        http_status_codes_to_retry (List[int]): List of status codes to retry
        fn (Callable[[x],x]: Function to call on successful connection
        **kwargs
    """
    backoff_interval = interval
    raised_exc = None

    if method not in ["get", "patch", "post"]:
        raise ValueError

    if retries == -1:  # -1 means retry indefinitely
        attempt = -1
    elif retries == 0:  # Zero means don't retry
        attempt = 1
    else:  # any other value means retry N times
        attempt = retries + 1

    while attempt != 0:
        if raised_exc:
            logger.error(
                f'Caught "{raised_exc}" url:{url} method:{method.upper()}, remaining tries {attempt}, '
                "sleeping {backoff_interval:.2f}secs"
            )
            await asyncio.sleep(backoff_interval)
            # bump interval for the next possible attempt
            backoff_interval *= backoff
        # logger.info(f'sending {method.upper()} {url} with {kwargs}')
        try:
            async with await getattr(session, method)(
                url, **kwargs
            ) as response:
                if response.status == 200:
                    return await fn(response)
                elif response.status in http_status_codes_to_retry:
                    logger.error(
                        f'Received invalid response code:{response.status} error:{""}'
                        f" response:{response.reason} url:{url}"
                    )
                    raise aiohttp.ClientResponseError(
                        code=response.status,
                        message=response.reason,
                        request_info=response.request_info,
                        history=response.history,
                    )
                else:
                    raise FailedRequest(
                        code=response.status,
                        message="Non-retryable response code",
                        raised="aiohttp.ClientResponseError",
                        url=url,
                    )
        except aiohttp.ClientError as exc:
            try:
                code = exc.code
            except AttributeError:
                code = ""
            raised_exc = FailedRequest(
                code=code,
                message=str(exc),
                raised=f"{exc.__class__.__module__}.{exc.__class__.__qualname__}",
                url=url,
            )
        except asyncio.TimeoutError as exc:
            raised_exc = FailedRequest(
                code="",
                message="asyncio.TimeoutError",
                raised=f"{exc.__class__.__module__}.{exc.__class__.__qualname__}",
                url=url,
            )
        else:
            raised_exc = None
            break

        attempt -= 1

    if raised_exc:
        raise raised_exc
