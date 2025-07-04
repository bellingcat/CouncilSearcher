from typing import TypedDict


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
    parsed_transcript: (
        list[tuple[str, str, str]] | None
    )  # List of tuples with start time, end time, and text
