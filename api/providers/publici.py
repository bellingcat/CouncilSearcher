from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from bs4 import BeautifulSoup
from bs4 import Tag
import requests
from tqdm import tqdm

from api.models.meetings import MeetingItem, AgendaItem
from api.providers.provider import Provider
from api.utils.parsing import parse_vtt
from api.utils.http import get_text, get_xml_dict


class PublicI(Provider):
    """
    Provider for PublicI meetings.
    """

    def build_index(self) -> list[MeetingItem]:
        """
        Build the index for PublicI provider.
        """

        ## Resolve the URLs for the authority
        self._transcript_url_template, rss_url = self._resolve_urls()

        ## Get the list of meetings from the RSS feed
        feed = get_xml_dict(rss_url)
        unparsed_index = feed["rss"]["channel"].get("item", None)
        if unparsed_index is None:
            return []
        with ThreadPoolExecutor() as executor:
            results = list(
                tqdm(
                    executor.map(self._build_index_item, unparsed_index),
                    total=len(unparsed_index),
                )
            )

        return results

    def get_meetings(self, index: list[MeetingItem]) -> list[MeetingItem]:
        """
        Get meetings for PublicI provider.
        """

        with ThreadPoolExecutor() as executor:
            results = list(
                tqdm(
                    executor.map(self._parse_index_item, index),
                    total=len(index),
                )
            )

        return results

    def _resolve_urls(self) -> tuple[str, str]:
        authority = self.authority
        # use the magic_rss page to get the authority id (termed ds_id in the page)
        magic_rss_page_url = f"https://{authority}.public-i.tv/core/portal/magic_rss"
        print(f"Getting magic RSS feed for authority {authority}: {magic_rss_page_url}")
        response = requests.get(magic_rss_page_url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch magic RSS feed: {response.status_code}")
        page_content = response.content

        # Use BeautifulSoup to parse the HTML and extract the ds_id
        soup = BeautifulSoup(page_content, "html.parser")
        ds_id_input = soup.find("input", {"name": "ds_id"})
        authority_id = ds_id_input["value"] if isinstance(ds_id_input, Tag) else None
        if authority_id == None:
            raise Exception(
                "Could not find ds_id input or value in the magic RSS page."
            )
        print(f"Getting data for authority {authority} with id {authority_id}")

        # Construct urls for transcripts and database
        transcript_url = (
            f"https://cl-assets.public-i.tv/{authority}/subtitles/{authority}_"
            + "{uid}_en_GB.vtt"
        )
        rss_url = f"https://{authority}.public-i.tv/core/data/{authority_id}/archived/1/agenda/1"
        return transcript_url, rss_url

    def _build_index_item(self, full_item: dict) -> MeetingItem:
        title = full_item.get("title")
        description = full_item.get("description")
        tags = full_item.get("pi:tags")
        date = full_item.get("pi:liveDate")

        # Convert the date to a Unix timestamp
        unix_time = (
            int(datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %z").timestamp())
            if date
            else None
        )
        date_time = (
            datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %z").isoformat(" ")
            if date
            else None
        )

        link = full_item.get("guid", "")

        agenda_items = (full_item.get("pi:agenda", {}) or {}).get("pi:agenda_item", [])
        if isinstance(agenda_items, dict):
            agenda_items = [agenda_items]

        agenda = [
            AgendaItem(
                id=agenda_item.get("pi:agenda_id"),
                text=agenda_item.get("pi:agenda_text"),
                time=agenda_item.get("pi:agenda_time"),
            )
            for agenda_item in agenda_items
        ]
        uid = str(full_item.get("pi:activity", ""))
        if uid != link.split("/")[-1]:
            print(f"UID {uid} does not match link {link}")

        transcript_url = self._transcript_url_template.format(uid=uid)

        result = MeetingItem(
            title=title,
            description=description,
            tags=tags,
            date=date,
            unixtime=unix_time,
            datetime=date_time,
            link=link,
            agenda=agenda,
            uid=uid,
            transcript_url=transcript_url,
            transcript=None,
            parsed_transcript=None,
        )
        return result

    def _parse_index_item(self, index_item: MeetingItem) -> MeetingItem:

        transcript_url = index_item.get("transcript_url")
        if transcript_url is not None:
            transcript = get_text(transcript_url)
            parsed_transcript = parse_vtt(transcript) if transcript else None
        else:
            transcript = None
            parsed_transcript = None

        meeting_item = index_item.copy()
        meeting_item.update(
            transcript=transcript,
            parsed_transcript=parsed_transcript,
        )

        return meeting_item
