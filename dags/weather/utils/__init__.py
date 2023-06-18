"""
Utils package for weather DAG.

Constains utility functions for web scraping weather data.
"""

import requests


def get_weather_data(cities: list[str]):
    """
    Get weather data from the internet.
    """

    for city in cities:
        url = f"https://www.meteored.mx/{city}/historico"
        response = requests.get(url)
        print(response.json())
