"""Extracts data from Google Play reviews."""

from __future__ import annotations

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_google_play import streams


class TapGooglePlay(Tap):
    """GooglePlay tap class."""
    name = "tap-googleplay"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "app_id",
            th.StringType,
            required=True
        ),
        th.Property(
            "countries",
            th.ArrayType(th.StringType),
            required=False
        ),
        th.Property(
            "languages",
            th.ArrayType(th.StringType),
            required=False
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False
        )
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            streams.ReviewsStream(tap=self),
        ]


if __name__ == "__main__":
    TapGooglePlay.cli()
