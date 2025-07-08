import json
from pathlib import Path

import sqlite3

from api.models.meetings import MeetingItem

DB_PATH = DB_PATH = (
    Path(__file__).parent.parent / "data" / "council_meetings.db"
).resolve()


def create_database() -> None:
    # Create tables
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS providers (
                id TEXT PRIMARY KEY,
                config TEXT
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS authorities (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                nice_name Text,
                FOREIGN KEY (provider) REFERENCES providers(id)
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meetings (
                uid TEXT PRIMARY KEY,
                authority TEXT,
                title TEXT,
                description TEXT,
                datetime TEXT,
                unixtime INTEGER,
                link TEXT,
                FOREIGN KEY (authority) REFERENCES authorities(id)
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transcripts (
                uid TEXT,
                transcript TEXT,
                title TEXT,
                description TEXT,
                start_time TEXT,
                end_time TEXT,
                PRIMARY KEY (uid, start_time, end_time),
                FOREIGN KEY (uid) REFERENCES meetings(uid)
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agenda (
                uid TEXT,
                agenda_id TEXT,
                agenda_text TEXT,
                agenda_time TEXT,
                PRIMARY KEY (uid, agenda_id),
                FOREIGN KEY (uid) REFERENCES meetings(uid)
            )
        """
        )
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS transcripts_fts USING fts5(
                uid,
                transcript,
                tokenize='porter'
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS offsets (
                uid TEXT,
                offset INTEGER,
                start_time TEXT,
                start_time_seconds INTEGER,
                PRIMARY KEY (uid, offset),
                FOREIGN KEY (uid) REFERENCES meetings(uid)
            )
        """
        )
    return


def add_meetings_to_db(authority: str, meetings: list[MeetingItem]) -> None:
    """
    Add meetings to the database.
    """

    directory = {item["uid"]: item for item in meetings}

    # Insert data into tables
    with sqlite3.connect(DB_PATH) as conn:
        # Insert authority data
        conn.execute(
            """
                INSERT OR IGNORE INTO authorities (id)
                VALUES (?)
            """,
            (authority,),
        )
        for uid, item in directory.items():
            conn.execute(
                """
                    INSERT OR IGNORE INTO meetings (uid, authority, title, description, datetime, unixtime, link)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uid,
                    authority,
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

            full_transcript = " ".join(text for _, _, text in item["parsed_transcript"])

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


def get_transcript_and_meeting_counts(authority: str) -> tuple[int, int]:

    with sqlite3.connect(DB_PATH) as conn:
        # Compare number of transcripts_fts and meetings
        cursor = conn.execute(
            """
            SELECT COUNT(*) FROM transcripts_fts WHERE uid IN (
                SELECT uid FROM meetings WHERE authority = ?
            )
        """,
            (authority,),
        )

        transcripts_count = cursor.fetchone()[0]

        cursor = conn.execute(
            """
            SELECT COUNT(*) FROM meetings WHERE authority = ?
        """,
            (authority,),
        )

        meetings_count = cursor.fetchone()[0]

    return transcripts_count, meetings_count


def get_authorities_and_transcript_counts() -> dict[str, int]:
    """
    Get a list of authorities and their corresponding number of available transcripts.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT authority, COUNT(*) as transcript_count
            FROM authorities
            JOIN meetings ON authorities.id = meetings.authority
            WHERE meetings.uid IN (
                SELECT uid FROM transcripts_fts
            )
            GROUP BY authority
        """
        )
        authorities = {row[0]: row[1] for row in cursor.fetchall()}
    return authorities


def build_query_string(
    query: str,
    authority: list[str] | None = None,
    startdate: str | None = None,
    enddate: str | None = None,
) -> tuple[str, list[str]]:

    params = [query]

    # Search for the phrase in the transcripts using FTS
    query_string = """
        SELECT uid, snippet(transcripts_fts, 1, '[', ']', '', 70) AS snippet, rank, transcript
        FROM transcripts_fts
        WHERE transcript MATCH ?
    """

    # Filter by authority if provided
    if authority:
        query_string += """
            AND uid IN (
                SELECT uid FROM meetings WHERE authority IN ({})
            )
        """.format(
            ",".join("?" for _ in authority)
        )
        params.extend(authority)

    # Filter by startdate and enddate if provided
    if startdate:
        query_string += """
            AND uid IN (
                SELECT uid FROM meetings WHERE datetime >= ?
            )
        """
        params.append(startdate)

    if enddate:
        query_string += """
            AND uid IN (
                SELECT uid FROM meetings WHERE datetime <= ?
            )
        """
        params.append(enddate)

    query_string += " ORDER BY bm25(transcripts_fts)"
    return query_string, params


def search_meetings(
    query: str,
    authority: list[str] | None = None,
    startdate: str | None = None,
    enddate: str | None = None,
) -> list[dict]:
    """
    Search for meetings based on the query string.
    Returns a list of dictionaries with meeting details.
    """

    query_string, params = build_query_string(
        query=query,
        authority=authority,
        startdate=startdate,
        enddate=enddate,
    )

    with sqlite3.connect(DB_PATH) as conn:
        results = conn.execute(query_string, params).fetchall()

        formatted_results = []
        for result in results:
            # Calculate the start time from the offset
            uid, snippet, rank, transcript = result
            # Use the matched snippet to find the offset in the full transcript
            snippet_cleaned = snippet.replace("[", "").replace("]", "")

            offset = transcript.find(snippet_cleaned)

            # Get the largest offset less than the current offset
            cursor = conn.execute(
                """
                SELECT start_time, start_time_seconds FROM offsets
                WHERE uid = ? AND offset <= ? ORDER BY offset DESC LIMIT 1
            """,
                (uid, offset),
            )

            start_time, start_time_seconds = cursor.fetchone()

            # Fetch the link from the meetings table matching the uid
            meeting_cursor = conn.execute(
                """
                SELECT title, datetime, unixtime, link, authority FROM meetings
                WHERE uid = ?
            """,
                (uid,),
            )
            meeting_title, datetime, unixtime, meeting_link, authority_result = (
                meeting_cursor.fetchone()
            )
            meeting_link = f"{meeting_link}/start_time/{1000*start_time_seconds}"

            formatted_results.append(
                {
                    "title": meeting_title,
                    "datetime": datetime,
                    "unixtime": unixtime,
                    "snippet": snippet,
                    "start_time": start_time,
                    "rank": rank,
                    "link": meeting_link,
                    "authority": authority_result,
                }
            )

        return formatted_results

def add_provider(provider_id: str, config: dict | None = None) -> None:
    """
    Add a new provider to the database.
    """

    config_json = json.dumps(config) if config else None
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO providers (id, config)
            VALUES (?, ?)
        """,
            (provider_id, config_json),
        )


def add_authority(
    authority_id: str, provider_id: str, nice_name: str | None = None
) -> None:
    """
    Add a new authority to the database.
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO authorities (id, provider, nice_name)
            VALUES (?, ?, ?)
        """,
            (authority_id, provider_id, nice_name),
        )
