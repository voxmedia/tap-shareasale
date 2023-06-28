"""REST client handling, including shareasaleStream base class."""

from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from time import gmtime, strftime
from typing import Any, Callable, Iterable

import requests
from requests import Response
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class DayChunkPaginator(BaseAPIPaginator):
    """A paginator that increments days in a date range."""

    def __init__(
        self,
        start_date: str,
        increment: int = 1,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(start_date)
        self._value = datetime.strptime(start_date, "%Y-%m-%d")
        self._end = datetime.today()
        self._increment = increment

    @property
    def end_date(self):
        """Get the end pagination value.

        Returns:
            End date.
        """
        return self._end

    @property
    def increment(self):
        """Get the paginator increment.

        Returns:
            Increment.
        """
        return self._increment

    def get_next(self, response: Response):
        return (
            self.current_value + timedelta(days=self.increment)
            if self.has_more(response)
            else None
        )

    def has_more(self, response: Response) -> bool:
        """Checks if there are more days to process.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return (
            self.current_value
            + timedelta(
                days=self.increment,
            )
            < self.end_date
        )


def set_none_or_cast(value, expected_type):
    if value == "" or value is None:
        return None
    elif not isinstance(value, expected_type):
        return expected_type(value)
    else:
        return value


class ShareasaleStream(RESTStream):
    """shareasale stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://shareasale.com/x.cfm"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        api_token = self.config.get("auth_token")
        api_secret_key = self.config.get("api_secret_key")
        my_timestamp = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        action_verb = self.name
        sig = api_token + ":" + my_timestamp + ":" + action_verb + ":" + api_secret_key
        sig_hash = hashlib.sha256(sig.encode("utf-8")).hexdigest()
        headers["X-ShareASale-Date"] = my_timestamp
        headers["X-ShareASale-Authentication"] = sig_hash
        return headers

    def get_new_paginator(self) -> DayChunkPaginator:
        return DayChunkPaginator(start_date=self.config.get("start_date"), increment=28)

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any] | str:
        params: dict = {
            "affiliateId": self.config.get("affiliate_id"),
            "token": self.config.get("auth_token"),
            "version": self.config.get("api_version"),
            "action": self.name,
        }
        date_format_str = "%Y-%m-%d"
        next_page_date = datetime.strftime(next_page_token, date_format_str)
        if next_page_date:
            params["dateStart"] = next_page_date
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        f = StringIO(response.text)
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:  # noqa: UP028
            yield row

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        new_row = {}
        for key, value in row.items():
            new_row[key.replace(" ", "_")] = value
        return new_row
