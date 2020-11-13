import json
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import DefaultDict, NamedTuple, Set, Tuple

from haversine import haversine
from shapely.geometry.point import Point
import click
import holidays
import pytz
import us
from shapely.geometry import shape

UTC = pytz.utc
us_holidays = holidays.UnitedStates()
us_holidays.append(
    {
        date(2019, 12, 24): 'Christmas Eve',
        date(2019, 11, 28): 'Day After Thanksgiving'
    }
)

OFFICE_LOCATIONS = [
    (40.741238, -74.0008963),
    (40.7538528,-73.9968516)
]
OFFICE_DISTANCE_THRESHOLD_KM = 0.5


class ParsedVisit(NamedTuple):
    lat: float
    lng: float
    state: str
    start_date: date
    end_date: date
    near_office: bool 


def load_states_geojson(geojson_file: Path):
    result = {}
    states_geojson = json.loads(geojson_file.read_text(encoding='utf8'))
    for feature in states_geojson['features']:
        result[feature['properties']['NAME']] = shape(feature['geometry'])
    return list(result.items())


def find_state(point, states_geojson):
    p = Point(point[1], point[0])
    for index, (state_name, state_shape) in enumerate(states_geojson):
        if p.within(state_shape):
            states_geojson.insert(0, states_geojson.pop(index))
            return state_name
    return None


def parse_place_visit(visit, timezone, states_geojson):
    location = visit.get('location', {})
    lat, lng = location['latitudeE7'] / 1e7, location['longitudeE7'] / 1e7
    near_office = False
    start_timestamp = datetime.fromtimestamp(int(visit['duration']['startTimestampMs']) / 1000, UTC)
    end_timestamp = datetime.fromtimestamp(int(visit['duration']['endTimestampMs']) / 1000, UTC)
    state = find_state((lat, lng), states_geojson) or ''
    if state:
        for office_location in OFFICE_LOCATIONS:
            near_office = near_office or (haversine((lat, lng), office_location) < OFFICE_DISTANCE_THRESHOLD_KM)
    return ParsedVisit(
        lat, 
        lng, 
        state.strip(), 
        start_timestamp.astimezone(timezone).date(), 
        end_timestamp.astimezone(timezone).date(),
        near_office
    )


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def is_holiday(d: date) -> bool:
    return d in us_holidays or d.weekday() in [5, 6]


def parse_semantic_location_file(month, states_geojson, timezone, result):
    timeline_objects = month['timelineObjects']
    visits = []
    for timeline_object in timeline_objects:
        visit = timeline_object.get('placeVisit', {})
        location = visit.get('location', {})
        if location:
            parsed_visit = parse_place_visit(visit, timezone, states_geojson)
            if parsed_visit:
                for d in daterange(parsed_visit.start_date, parsed_visit.end_date):
                    result[d].add((parsed_visit.state, parsed_visit.near_office))
        

def parse_semantic_year(takeout_dir: Path, year: int, states_geojson, timezone, result: DefaultDict[date, Set[Tuple[str, bool]]]) -> None:
    semantic_location_dir = takeout_dir / 'Location History' / 'Semantic Location History' / str(year)
    for month_file in semantic_location_dir.glob(f'{year}_*.json'):
        parse_semantic_location_file(json.loads(month_file.read_text(encoding='utf8')), states_geojson, timezone, result)


def count_state_days(takeout_dir: Path, start_date: date, end_date: date, state: str, states_geojson):
    summary = defaultdict(int)
    details = []
    visit_map = defaultdict(set)
    years = set()
    timezone = pytz.timezone(us.states.lookup(state).capital_tz)
    for d in daterange(start_date, end_date):
        in_state = False
        working = False
        if d.year not in years:
            parse_semantic_year(takeout_dir, d.year, states_geojson, timezone, visit_map)
            years.add(d.year)
        if d not in visit_map:
            if not is_holiday(d):
                # No data - assume went to the office if not holiday
                in_state = True
                working = True
        else:
            visit_day = visit_map[d]
            if (state, True) in visit_day:
                in_state = True
                working = not is_holiday(d)
            elif (state, False) in visit_day:
                in_state = True
                working = False
        if in_state:
            summary[working] += 1
            details += [d, working]
    return summary, details


@click.command()
@click.option('--takeout-dir', required=True, help='Path to "Google Takeout" directory')
@click.option('--states-geojson', required=True, help='Path to states geojson file (Downlaod from https://eric.clst.org/tech/usgeojson/)')
@click.option('--state', required=True, help='State performing audit')
@click.option('--year', required=True, help='Year being audited')
def days_in_state(takeout_dir, states_geojson, state, year):
    states_geojson = load_states_geojson(Path(states_geojson))
    summary, details = count_state_days(Path(takeout_dir), date(int(year), 1, 1),  date(int(year), 12, 31), state, states_geojson)
    click.echo(f'Report for year {year} in {state}:')
    click.echo(f'\tDays Working in {state}: {summary[True]}')
    click.echo(f'\tDays Not Working in {state}: {summary[False]}')


if __name__ == '__main__':
    days_in_state()