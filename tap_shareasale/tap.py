"""shareasale tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_shareasale import streams


class Tapshareasale(Tap):
    """shareasale tap class."""

    name = "tap-shareasale"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "affiliate_id",
            th.StringType,
            required=True,
            description="Affiliate ID",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_secret_key",
            th.StringType,
            required=True,
            description="Secret key for the API service",
            secret=True,
        ),
        th.Property(
            "api_version",
            th.StringType,
            description="API version to use for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.shareasaleStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ActivityStream(self)
        ]


if __name__ == "__main__":
    Tapshareasale.cli()
