"""
Utils package for weather DAG.

Constains utility functions for web scraping weather data.
"""

from typing import List
import re
import requests


def get_span_value(
    html: str,
    span_id: str,
):
    """
    Get value from a span tag by its id.
    """
    match = re.search(
        r'<span id="{}">(.*?)</span>'.format(span_id),
        html,
    )

    if match:
        return match.group(1)
    else:
        return None


def get_weather_data(
    cities: List[str],
    span_id_tags: dict,
):
    """
    Get weather data from the internet.
    """
    info = {}

    for city in cities:
        info[city] = {}
        response = requests.get(f"https://www.meteored.mx/{city}/historico")
        for tag, span_id in span_id_tags.items():
            if response.status_code == 200:
                info[city][tag] = get_span_value(
                    html=response.text,
                    span_id=span_id,
                )
            else:
                info[city][tag] = None

    return info
