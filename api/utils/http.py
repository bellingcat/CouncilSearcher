import requests
import xmltodict


def get_xml_dict(url: str) -> dict:
    """Fetch some XML (like an RSS feed) from the given URL and return the parsed data."""
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch the XML: {response.status_code}")

    return xmltodict.parse(response.content)


def get_text(url: str) -> str | None:
    """Fetch the transcript from the given URL and return the parsed data."""
    response = requests.get(url)
    if response.status_code != 200:
        return None

    return response.text
