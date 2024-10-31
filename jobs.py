import os
import csv
from io import StringIO
from django.db import transaction
from nautobot.apps.jobs import Job, register_jobs, RunJobTaskFailed, FileVar
from nautobot.dcim.models import Location
from nautobot.core.api.utils import get_serializer_for_model
from nautobot.core.api.parsers import NautobotCSVParser
from nautobot.core.exceptions import AbortTransaction
from rest_framework import exceptions as drf_exceptions

# Ideally we use some sort of python library for this but ChatGPT spits this out pretty quick.
state_abbreviations = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


class LocationImport(Job):
    """Imports locations from a CSV file."""

    input_file = FileVar(label="Hardware CSV File", required=True)

    class Meta:  # pylint: disable=too-few-public-methods
        """Meta class."""

        name = "Location Import"
        description = "Locationj Import."

    def get_location_type_from_name(self, name):
        """Get the location type object from the name."""
        if name.endswith("DC"):
            return "Data Center"
        elif name.endswith("BR"):
            return "Branch"
        else:
            raise Exception("Site name does not end with `DC` or `BR`.")

    def edit_csv(self, rows):
        """Edit the CSV data."""
        new_rows = []
        # Add header row
        new_rows.append(["name", "location_type__name", "status", "parent__name"])

        for row in rows[1:]:
            for entry in row.split(","):
                new_rows.append([entry[2], "State", "Active", ""])
                new_rows.append([entry[1], "City", "Active", entry[2]])
                new_rows.append(
                    [
                        entry[0],
                        self.get_location_type_from_name(entry[0]),
                        "Active",
                        entry[1],
                    ]
                )

        with open(
            f"{os.path.join(os.path.dirname(__file__))}/normalized_locations.csv",
            "w",
            newline="",
            encoding="utf-8",
        ) as outfile:
            writer = csv.writer(outfile)
            writer.writerows(new_rows)

    def _perform_operation(self, data, serializer_class, queryset):
        new_objs = []
        validation_failed = False
        for row, entry in enumerate(data, start=1):
            serializer = serializer_class(data=entry, context={"request": None})
            if serializer.is_valid():
                try:
                    with transaction.atomic():
                        new_obj = serializer.save()
                        if not queryset.filter(pk=new_obj.pk).exists():
                            raise AbortTransaction()
                    self.logger.info(
                        'Row %d: Created record "%s"',
                        row,
                        new_obj,
                        extra={"object": new_obj},
                    )
                    new_objs.append(new_obj)
                except AbortTransaction:
                    self.logger.error(
                        'Row %d: User "%s" does not have permission to create an object with these attributes',
                        row,
                        self.user,
                    )
                    validation_failed = True
            else:
                validation_failed = True
                for field, err in serializer.errors.items():
                    self.logger.error("Row %d: `%s`: `%s`", row, field, err[0])
        return new_objs, validation_failed

    def import_csv(self, filename, model):
        serializer_class = get_serializer_for_model(model)
        queryset = model.objects.restrict(self.user, "add")

        with open(
            f"{os.path.join(os.path.dirname(__file__))}/{filename}.csv",
            "rb",
        ) as csv_bytes:
            new_objs = []
            try:
                data = NautobotCSVParser().parse(
                    stream=csv_bytes,
                    parser_context={
                        "request": None,
                        "serializer_class": serializer_class,
                    },
                )
                self.logger.info("Processing %d rows of data", len(data))
                new_objs, validation_failed = self._perform_operation(
                    data, serializer_class, queryset
                )
            except drf_exceptions.ParseError as exc:
                validation_failed = True
                self.logger.error("`%s`", exc)

            if new_objs:
                self.logger.info(
                    "Created %d %s object(s) from %d row(s) of data",
                    len(new_objs),
                    model,
                    len(data),
                )
            else:
                self.logger.warning("No %s objects were created", model)

            if validation_failed:
                raise RunJobTaskFailed("CSV import not fully successful, see logs")

    def run(self, input_file):
        """Entrypoint for job."""
        text = input_file.read().decode("utf-8")
        rows = text.splitlines()
        csv_rows = [row.split(",") for row in rows]
        self.edit_csv(csv_rows)
        self.import_csv("normalized_locations", Location)

        # Return newly created CSV
        buffer = StringIO()
        writer = csv.writer(buffer)
        with open(
            f"{os.path.join(os.path.dirname(__file__))}/normalized_locations.csv",
            "r",
            newline="",
            encoding="utf-8",
        ) as outfile:
            for row in outfile:
                writer.writerow(row.split(","))

        csv_data = buffer.getvalue()
        self.create_file("normalized_locations.csv", csv_data)

        os.remove(f"{os.path.join(os.path.dirname(__file__))}/normalized_locations.csv")


register_jobs(LocationImport)
