"""
Utils package for weather DAG.

Constains utility functions for web scraping weather data.
"""

import requests

from typing import List


def get_weather_data(cities: List[str]):
    """
    Get weather data from the internet.
    """

    for city in cities:
        response = requests.get(f"https://www.meteored.mx/{city}/historico")
        print(response.json())
