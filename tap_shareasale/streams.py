"""Stream type classes for tap-shareasale."""

from __future__ import annotations

import pendulum
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_shareasale.client import ShareasaleStream


def set_none_or_cast(value, expected_type):
    if value == "" or value is None:
        return None
    elif not isinstance(value, expected_type):
        return expected_type(value)
    else:
        return value


class ActivityStream(ShareasaleStream):
    """Define custom stream."""

    name = "activity"
    path = ""
    schema = th.PropertiesList(
        th.Property("Trans_ID", th.IntegerType),
        th.Property(
            "User_ID",
            th.IntegerType,
        ),
        th.Property(
            "Merchant_ID",
            th.IntegerType,
        ),
        th.Property(
            "Trans_Date",
            th.DateTimeType,
        ),
        th.Property(
            "Trans_Amount",
            th.NumberType,
        ),
        th.Property(
            "Commission",
            th.NumberType,
        ),
        th.Property(
            "Comment",
            th.StringType,
        ),
        th.Property(
            "Voided",
            th.IntegerType,
        ),
        th.Property(
            "Pending_Date",
            th.StringType,
        ),
        th.Property(
            "Locked",
            th.IntegerType,
        ),
        th.Property(
            "Aff_Comment",
            th.StringType,
        ),
        th.Property(
            "Banner_Page",
            th.StringType,
        ),
        th.Property(
            "Reversal_Date",
            th.DateTimeType,
        ),
        th.Property(
            "Click_Date",
            th.DateTimeType,
        ),
        th.Property(
            "Click_Time",
            th.StringType,
        ),
        th.Property(
            "Banner_Id",
            th.StringType,
        ),
        th.Property(
            "SKU_List",
            th.StringType,
        ),
        th.Property(
            "Quantity_List",
            th.StringType,
        ),
        th.Property(
            "Lock_Date",
            th.DateTimeType,
        ),
        th.Property(
            "Paid_Date",
            th.DateType,
        ),
        th.Property(
            "Merchant_Organization",
            th.StringType,
        ),
        th.Property(
            "Merchant_Website",
            th.StringType,
        ),
        th.Property(
            "Trans_Type",
            th.StringType,
        ),
        th.Property(
            "Merchant_Defined_Type",
            th.IntegerType,
        ),
        th.Property(
            "Store_ID",
            th.IntegerType,
        ),
        th.Property(
            "Reference_Trans",
            th.StringType,
        ),
    ).to_dict()

    @property
    def next_page_token(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("start_date", "")

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        new_row = super().post_process(row, context)
        for field_tuple in [
            ("Trans_ID", int),
            ("User_ID", int),
            ("Merchant_ID", int),
            ("Trans_Amount", float),
            ("Commission", float),
            ("Voided", int),
            ("Locked", int),
            ("Merchant_Defined_Type", int),
            ("Store_ID", int),
        ]:
            field_name = field_tuple[0]
            field_type = field_tuple[1]
            if field_name in new_row:
                new_row[field_name] = set_none_or_cast(new_row[field_name], field_type)
        for field_tuple in [
            ("Trans_Date", "MM/DD/YYYY hh:mm:ss A"),
            ("Reversal_Date", "YYYY-MM-DD HH:mm:ss.S"),
            ("Click_Date", "YYYY-MM-DD HH:mm:ss.SSS"),
            ("Lock_Date", "YYYY-MM-DD"),
        ]:
            if (
                field_tuple[0] in new_row
                and new_row[field_tuple[0]] is not None
                and new_row[field_tuple[0]] != ""
            ):
                new_row[field_tuple[0]] = pendulum.from_format(
                    new_row[field_tuple[0]],
                    field_tuple[1],
                ).format(
                    "YYYY-MM-DD HH:mm:ss",
                )
        new_row["Reference_Trans"] = ""
        return new_row
