import csv
from itertools import count
import json
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta, date, tzinfo
from pathlib import Path
from typing import (
    DefaultDict,
    Dict,
    NamedTuple,
    Set,
    Tuple,
    Iterable,
    List,
    Optional,
    TypedDict,
)


import click
import dateutil.parser
import holidays
from holidays.holiday_base import HolidayBase
import pytz
from shapely.geometry.polygon import Polygon
import us
from haversine import haversine
from shapely.geometry.point import Point
from shapely.geometry import shape

UTC = pytz.utc


def daterange(start_date: date, end_date: date) -> Iterable[date]:
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


class Location(NamedTuple):
    lat: float
    lng: float


class ParsedVisit(NamedTuple):
    location: Location
    state: str
    start_date: date
    end_date: date
    near_office: bool


class LocationVisit(TypedDict):
    latitudeE7: int
    longitudeE7: int
    placeId: str
    address: str
    name: str
    locationConfidence: float


class SegmentDuration(TypedDict):
    startTimestampMs: int
    endTimestampMs: int


class PlaceVisit(TypedDict):
    location: LocationVisit
    duration: SegmentDuration


class Waypoint(TypedDict):
    latE7: int
    lngE7: int


class Waypoints(TypedDict):
    waypoints: List[Waypoint]


class ActivitySegment(TypedDict):
    startLocation: LocationVisit
    endLocation: LocationVisit
    waypointPath: Waypoints
    duration: SegmentDuration


class TimelineObject(TypedDict):
    placeVisit: PlaceVisit
    activitySegment: ActivitySegment


class TimelineMonth(TypedDict):
    timelineObjects: List[TimelineObject]


class Geocoder:
    def __init__(
        self,
        states_geojson_file: Path,
        countries_geojson_file: Path,
        office_locations: List[Location],
        office_distance_threshold_km: float = 0.75,
    ) -> None:
        self.states_geojson: List[Tuple[str, Polygon]] = self._load_geojson(
            states_geojson_file, "NAME"
        )
        self.countries_geojson: List[Tuple[str, Polygon]] = self._load_geojson(
            countries_geojson_file, "ADMIN"
        )
        self.office_locations = office_locations
        self.office_distance_threshold_km = office_distance_threshold_km

    @staticmethod
    def _load_geojson(
        geojson_file: Path,
        name_key: str,
    ) -> List[Tuple[str, Polygon]]:
        result = {}
        states_geojson = json.loads(geojson_file.read_text(encoding="utf8"))
        for feature in states_geojson["features"]:
            result[feature["properties"][name_key]] = shape(feature["geometry"]).buffer(
                0.005
            )
        return list(result.items())

    def find_state(self, location: Location) -> Optional[str]:
        p = Point(location.lng, location.lat)
        for index, (state_name, state_shape) in enumerate(self.states_geojson):
            if p.within(state_shape):
                # Keep most visited states towards the top
                self.states_geojson.insert(0, self.states_geojson.pop(index))
                return state_name
        for index, (country_name, country_shape) in enumerate(self.countries_geojson):
            if p.within(country_shape):
                # Keep most visited states towards the top
                self.countries_geojson.insert(0, self.countries_geojson.pop(index))
                return f"Outside US/{country_name}"
        return None

    def is_near_office(self, location: Location) -> bool:
        for office_location in self.office_locations:
            if haversine(location, office_location) < self.office_distance_threshold_km:
                return True
        return False


class Calendar:
    def __init__(self, working_holidays: List[str]) -> None:
        self._holiday_cache: Dict[int, HolidayBase] = {}
        self.working_holidays = working_holidays

    def _populate_year(self, year: int) -> None:
        holidays_for_year = holidays.US(years=year)
        for working_holiday in self.working_holidays:
            holidays_for_year.pop_named(working_holiday)
        thanksgiving: date = holidays_for_year.get_named("Thanksgiving")[0]
        holidays_for_year[thanksgiving + timedelta(days=1)] = "Day after Thanksgiving"
        christmas_eve = date(year, 12, 24)
        if christmas_eve.weekday() == 5:
            christmas_eve = date(year, 12, 23)
        if christmas_eve.weekday() == 6:
            christmas_eve = date(year, 12, 22)
        holidays_for_year[christmas_eve] = "Christmas Eve"
        self._holiday_cache[year] = holidays_for_year

    def is_working_day(self, d: date) -> bool:
        if d.year not in self._holiday_cache:
            self._populate_year(d.year)
        return d.weekday() not in [5, 6] and d not in self._holiday_cache[d.year]


class TakeoutParser:
    def __init__(
        self,
        geocoder: Geocoder,
        calendar: Calendar,
        timezone: tzinfo,
        takeout_dir: Path,
    ) -> None:
        self.geocoder = geocoder
        self.calendar = calendar
        self.timezone = timezone
        self.takeout_dir = takeout_dir

    def parse_place_visit(self, visit: PlaceVisit) -> ParsedVisit:
        visit_location = visit["location"]
        location = Location(
            visit_location["latitudeE7"] / 1e7,
            visit_location["longitudeE7"] / 1e7,
        )
        near_office = False
        start_timestamp = datetime.fromtimestamp(
            int(visit["duration"]["startTimestampMs"]) / 1000, UTC
        )
        end_timestamp = datetime.fromtimestamp(
            int(visit["duration"]["endTimestampMs"]) / 1000, UTC
        )
        state = self.geocoder.find_state(location) or ""
        near_office = self.geocoder.is_near_office(location)

        if start_timestamp.astimezone(self.timezone).date() == date(2016, 10, 21):
            print(json.dumps(visit))
        return ParsedVisit(
            location,
            state.strip(),
            start_timestamp.astimezone(self.timezone).date(),
            end_timestamp.astimezone(self.timezone).date(),
            near_office,
        )

    def parse_activity(self, visit: ActivitySegment) -> ParsedVisit:
        start = visit["startLocation"]
        end = visit["endLocation"]
        start_location = Location(
            start["latitudeE7"] / 1e7,
            start["longitudeE7"] / 1e7,
        )
        end_location = Location(
            end["latitudeE7"] / 1e7,
            end["longitudeE7"] / 1e7,
        )
        near_office = False
        start_timestamp = datetime.fromtimestamp(
            int(visit["duration"]["startTimestampMs"]) / 1000, UTC
        )
        end_timestamp = datetime.fromtimestamp(
            int(visit["duration"]["endTimestampMs"]) / 1000, UTC
        )
        start_near_office = self.geocoder.is_near_office(start_location)
        start_state = self.geocoder.find_state(start_location) or ""
        end_near_office = self.geocoder.is_near_office(end_location)
        end_state = self.geocoder.find_state(end_location) or ""
        near_office = start_near_office or end_near_office
        if not near_office and "waypointPath" in visit:
            for waypoint in visit["waypointPath"]["waypoints"]:
                location = Location(waypoint["latE7"] / 1e7, waypoint["lngE7"] / 1e7)
                if self.geocoder.is_near_office(location):
                    near_office = True
                    break

        if start_timestamp.astimezone(self.timezone).date() == date(2016, 10, 21):
            print(json.dumps(visit))
        return ParsedVisit(
            start_location,
            start_state.strip() if start_near_office else end_state.strip(),
            start_timestamp.astimezone(self.timezone).date(),
            end_timestamp.astimezone(self.timezone).date(),
            near_office,
        )

    def parse_semantic_location_file(
        self,
        month: TimelineMonth,
        result: DefaultDict[date, Set[Tuple[str, bool]]],
    ) -> None:
        timeline_objects = month["timelineObjects"]
        for timeline_object in timeline_objects:
            if "placeVisit" in timeline_object:
                visit = timeline_object["placeVisit"]
                if "location" in visit:
                    parsed_visit = self.parse_place_visit(visit)
                    result[parsed_visit.start_date].add(
                        (parsed_visit.state, parsed_visit.near_office)
                    )
            elif "activitySegment" in timeline_object:
                activity = timeline_object["activitySegment"]
                if (
                    "startLocation" in activity
                    and "latitudeE7" in activity["startLocation"]
                ):
                    parsed_visit = self.parse_activity(activity)
                    result[parsed_visit.start_date].add(
                        (parsed_visit.state, parsed_visit.near_office)
                    )

    def parse_semantic_year(
        self,
        year: int,
        result: DefaultDict[date, Set[Tuple[str, bool]]],
    ) -> None:
        semantic_location_dir = (
            self.takeout_dir
            / "Location History"
            / "Semantic Location History"
            / str(year)
        )
        for month_file in semantic_location_dir.glob(f"{year}_*.json"):
            self.parse_semantic_location_file(
                json.loads(month_file.read_text(encoding="utf8")),
                result,
            )

    def count_state_days(
        self,
        start_date: date,
        end_date: date,
        state: str,
    ) -> List[Tuple[date, str, bool]]:
        details: List[Tuple[date, str, bool]] = []
        visit_map: DefaultDict[date, Set[Tuple[str, bool]]] = defaultdict(set)
        years = set()
        last_state = state
        for d in daterange(start_date, end_date):
            if d.year not in years:
                self.parse_semantic_year(d.year, visit_map)
                years.add(d.year)
            if d not in visit_map:
                details.append((d, last_state, self.calendar.is_working_day(d)))
            else:
                visit_day = visit_map[d]
                for visits in visit_day:
                    if visits[1] and self.calendar.is_working_day(d):
                        details.append((d, visits[0], visits[1]))
                        last_state = visits[0]
                        break
                else:
                    details.append((d, visits[0], False))
        return details


OFFICE_LOCATIONS = [
    Location(37.760377, -122.413178),
    Location(47.605076, -122.336696),
    Location(47.605527, -122.337297),
    Location(45.528588, -122.663336),
    Location(39.786562, -104.918720),
    Location(36.163415, -86.776012),
    Location(34.052376, -118.255906),
    Location(40.741238, -74.0008963),
    Location(40.7538528, -73.9968516),
    Location(37.7759431, -122.391874),
    Location(45.5060197, -73.569546),
]


@click.command()
@click.option("--takeout-dir", required=True, help='Path to "Google Takeout" directory')
@click.option(
    "--states-geojson",
    required=True,
    help="Path to states geojson file (Downlaod from https://eric.clst.org/tech/usgeojson/)",
)
@click.option(
    "--countries-geojson",
    required=True,
    help="Path to countries geojson file (Downlaod from https://datahub.io/core/geo-countries)",
)
@click.option("--state", required=True, help="State performing audit")
@click.option("--csv-out", help="CSV Output")
@click.option("--start-date", required=False, help="First day to count")
@click.option("--end-date", required=False, help="Last day to count")
def days_in_state(
    takeout_dir: str,
    states_geojson: str,
    countries_geojson: str,
    state,
    csv_out,
    start_date,
    end_date,
):
    geocoder = Geocoder(Path(states_geojson), Path(countries_geojson), OFFICE_LOCATIONS)
    calendar = Calendar(["Columbus Day", "Veterans Day"])
    timezone = pytz.timezone(us.states.lookup(state).capital_tz)
    parser = TakeoutParser(geocoder, calendar, timezone, Path(takeout_dir))

    start = dateutil.parser.parse(start_date).date()
    end = dateutil.parser.parse(end_date).date()

    details = parser.count_state_days(
        start,
        end,
        state,
    )
    days_working = sum(x[2] for x in details)
    days_working_by_state = Counter(x[1] for x in details if x[2])
    click.echo(f"Report:")
    click.echo(f"\tTotal Days Worked: {days_working}")
    for state, days_worked in days_working_by_state.most_common():
        click.echo(f"\tTotal Days Worked in {state}: {days_worked}")
    if csv_out:
        with open(csv_out, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Date", "State", "Working"])
            for day in details:
                writer.writerow([day[0].strftime("%Y-%m-%d"), day[1], day[2]])


if __name__ == "__main__":
    days_in_state()
