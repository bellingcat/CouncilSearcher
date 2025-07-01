from typing import TypedDict
from tqdm import tqdm
import sqlite3
import requests
from bs4 import BeautifulSoup
from bs4 import Tag
import xmltodict
from concurrent.futures import ThreadPoolExecutor
import re
from datetime import datetime

class AgendaItem(TypedDict):
    id: str | None
    text: str | None
    time: str | None

class MeetingItem(TypedDict):
    title: str | None
    description: str | None
    tags: str | None  
    date: str | None  # Date in RFC 2822 format or None
    unixtime: int | None  # Unix timestamp or None
    datetime: str | None  # ISO format datetime or None
    link: str
    agenda: list[AgendaItem] | None  # List of agenda items with id, text, and time
    uid: str
    transcript_url: str | None
    transcript: str | None
    parsed_transcript: list[tuple[str, str, str]] | None  # List of tuples with start time, end time, and text


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


def parse_vtt(vtt_text: str) -> list[tuple[str, str, str]]:
    """Parse text from a VTT subtitle file and return a list of tuples with start time, end
    time, and text."""
    # Remove the VTT header and any styling information
    vtt_text = re.sub(r"WEBVTT.*?\n\n", "", vtt_text, flags=re.DOTALL)
    vtt_text = re.sub(r"<[^>]+>", "", vtt_text)  # Remove HTML tags

    # Split the text into lines and parse the timestamps and text
    lines = vtt_text.splitlines()
    parsed_lines = []
    for line in lines:
        if "-->" in line:
            start_time, end_time = line.split(" --> ")
            start_time = start_time.strip()
            end_time = end_time.strip()
        else:
            text = line.strip()
            if text:
                parsed_lines.append((start_time, end_time, text))

    return parsed_lines


class Provider:

    @staticmethod
    def create(
        provider_name: str, db_path: str, authority: str, config: dict | None = None
    ) -> "Provider":
        """
        Factory method to create a provider instance based on the provider name.
        """
        provider_classes = {
            "publici": PublicI,
        }

        if provider_name.lower() not in provider_classes:
            raise ValueError(f"Provider '{provider_name}' is not supported.")

        SpecificProvider = provider_classes[provider_name.lower()]

        return SpecificProvider(db_path=db_path, authority=authority, config=config)

    def __init__(self, db_path: str, authority: str, config: dict | None = None):
        self.db_path = db_path
        self.authority = authority
        self.config = config

        self.index: list | None = None

    def build_index(self) -> None:
        """
        Build the index for the provider.
        This method should be implemented by subclasses and should populate the `self.index` attribute.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def get_meetings(self) -> list[MeetingItem]:
        """
        Get meetings for the provider.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def add_meetings_to_db(self, meetings: list[MeetingItem]) -> None:
        """
        Add meetings to the database.
        """

        directory = {item["uid"]: item for item in meetings}

        # Insert data into tables
        with sqlite3.connect(self.db_path) as conn:
            # Insert authority data
            conn.execute(
                """
                INSERT OR IGNORE INTO authorities (id)
                VALUES (?)
            """,
                (self.authority,),
            )
            for uid, item in directory.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO meetings (uid, authority, title, description, datetime, unixtime, link)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        uid,
                        self.authority,
                        item["title"],
                        item["description"],
                        item["datetime"],
                        item["unixtime"],
                        item["link"],
                    ),
                )

                if item["parsed_transcript"]:
                    for start_time, end_time, text in item["parsed_transcript"]:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO transcripts (uid, transcript, title, description, start_time, end_time)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                uid,
                                text,
                                item["title"],
                                item["description"],
                                start_time,
                                end_time,
                            ),
                        )
                if item["agenda"] != None:
                    for agenda_item in item["agenda"]:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO agenda (uid, agenda_id, agenda_text, agenda_time)
                            VALUES (?, ?, ?, ?)
                        """,
                            (
                                uid,
                                agenda_item["id"],
                                agenda_item["text"],
                                agenda_item["time"],
                            ),
                        )

                if item["parsed_transcript"] == None:
                    continue

                full_transcript = " ".join(
                    text for _, _, text in item["parsed_transcript"]
                )

                offset = 0
                for start_time, end_time, text in item["parsed_transcript"]:
                    # Convert the start time from hh:mm:ss.sss to seconds
                    start_time_seconds = (
                        sum(
                            float(x) * 60**i
                            for i, x in enumerate(reversed(start_time.split(":")))
                        )
                        // 1
                    )

                    conn.execute(
                        """
                        INSERT OR IGNORE INTO offsets (uid, offset, start_time, start_time_seconds)
                        VALUES (?, ?, ?, ?)
                    """,
                        (uid, offset, start_time, start_time_seconds),
                    )
                    offset += len(text) + 1  # Add 1 for the space between words

                # Only insert the full transcript if it doesn't already exist
                # As virtual table doesn't support UNIQUE constraint
                conn.execute(
                    """
                    INSERT INTO transcripts_fts (uid, transcript)
                    SELECT ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM transcripts_fts WHERE uid = ?
                    )
                """,
                    (uid, full_transcript, uid),
                )


class PublicI(Provider):
    """
    Provider for PublicI meetings.
    """

    def build_index(self) -> None:
        """
        Build the index for PublicI provider.
        """

        ## Resolve the URLs for the authority
        self._transcript_url_template, rss_url = self._resolve_urls()

        ## Get the list of meetings from the RSS feed
        feed = get_xml_dict(rss_url)
        self.index = feed["rss"]["channel"].get("item", None)
        return

    def get_meetings(self) -> list[MeetingItem]:
        """
        Get meetings for PublicI provider.
        """
        if self.index is None:
            raise Exception("Index has not been built. Call build_index() first.")

        with ThreadPoolExecutor() as executor:
            results = list(
                tqdm(
                    executor.map(self._parse_index_item, self.index),
                    total=len(self.index),
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

    def _parse_index_item(self, full_item: dict) -> MeetingItem:
        item = {}
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

        transcript_url = self._transcript_url_template.format(uid=item["uid"])
        transcript = get_text(transcript_url)

        parsed_transcript = parse_vtt(transcript) if transcript else None

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
            transcript=transcript,
            parsed_transcript=parsed_transcript,
        )

        return result
