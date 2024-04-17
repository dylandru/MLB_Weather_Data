import pandas as pd
from meteostat import Stations, Daily
from datetime import datetime

def fetch_mlb_weather_data(start_year: int, end_year: int, output_file: str):

    '''
	param - start_year: integer clarifying start-year to get data
	param - end_year: integer clarifying end-year to get data
	param - output_file: string filepath where data will be saved in CSV format
	'''

    mlb_stadiums = {
        "Kauffman Stadium": (39.051910, -94.480682),
        "Guaranteed Rate Field": (41.830017, -87.634598),
        "Great American Ball Park": (39.097458, -84.507103),
        "Tropicana Field": (27.768284, -82.653961),
        "Target Field": (44.982075, -93.278435),
        "T-Mobile Park": (47.591480, -122.332863),
        "Coors Field": (39.756229, -104.994865),
        "Citizens Bank Park": (39.906216, -75.167465),
        "Citi Field": (40.757256, -73.846237),
        "Chase Field": (33.445564, -112.067413),
        "Progressive Field": (41.496262, -81.686043),
        "PNC Park, PA": (40.447105, -80.006363),
        "Oriole Park at Camden Yards": (39.284176, -76.622368),
        "Oracle Park": (37.778572, -122.389717),
        "RingCentral Coliseum": (37.751637, -122.201553),
        "Angel Stadium of Anaheim": (33.800560, -117.883438),
        "Nationals Park, Washington": (38.873055, -77.007996),
        "Truist Park": (33.890781, -84.468239),
        "Minute Maid Park": (29.757017, -95.356209),
        "American Family Field": (43.027954, -87.971497),
        "LoanDepot Park": (25.778301, -80.220352),
        "Dodger Stadium": (34.073814, -118.240784),
        "Yankee Stadium": (40.829659, -73.926186),
        "Fenway Park": (42.346268, -71.095764),
        "Wrigley Field": (41.947746, -87.656036),
        "Comerica Park": (42.338356, -83.048134),
        "Busch Stadium": (38.622780, -90.193329),
        "Globe Life Field": (32.7513, -97.0824),
        "Petco Park": (32.7076, -117.1566),
        "Rogers Centre": (43.6414, -79.3894)
    }

    # Specific station ids for stadiums that had trouble pulling data
    manual_stations = {
      "Guaranteed Rate Field": "KLXT0",
      "Oracle Park": 72494,
      "Minute Maid Park": 72244,
      "Wrigley Field": 72534,
    }

    expanded_data = pd.DataFrame()

    # Process stadium and year
    for year in range(start_year, end_year + 1):
        for stadium, coords in mlb_stadiums.items():
            start = datetime(year, 1, 1)
            end = datetime(year, 12, 31)

            # Gets Weather data based on proximity to station
            if stadium in manual_stations:
                station_id = manual_stations[stadium]
                data = fetch_weather_data(station_id, start, end)
            else:
                station_id, data = fetch_nearest_station_weather_data(coords, start, end)

            if not data.empty:
                data = process_weather_data(data, start, end, station_id, stadium)
                expanded_data = pd.concat([expanded_data, data], ignore_index=True)

    #Get to CSV
    expanded_data.to_csv(output_file, index=False)

def fetch_weather_data(station_id, start, end):
    data = Daily(station_id, start=start, end=end)
    data = data.normalize()
    return data.fetch()

def fetch_nearest_station_weather_data(coords, start, end):
    stations = Stations().nearby(*coords)
    stations = stations.inventory('daily', (start, end))
    stations = stations.fetch(10)
    
    for station_id in stations.index:
        data = fetch_weather_data(station_id, start, end)
        if not data.empty:
            return station_id, data
    return None, pd.DataFrame()  # If no data available

def process_weather_data(data, start, end, station_id, stadium):
    temp_df = pd.DataFrame({'date': pd.date_range(start, end)})
    temp_df['station_id'] = station_id
    temp_df['Stadium'] = stadium
    temp_df = pd.merge(temp_df, data, how='left', left_on='date', right_index=True)
    temp_df['tavg'] = (temp_df['tavg'] * 9/5) + 32
    temp_df['tmin'] = (temp_df['tmin'] * 9/5) + 32
    temp_df['tmax'] = (temp_df['tmax'] * 9/5) + 32
    return temp_df

# Example Call
fetch_mlb_weather_data(2021, 2023, "mlb_weather_data_2021_to_2023.csv")
