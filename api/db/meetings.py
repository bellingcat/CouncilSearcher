import json
from pathlib import Path
import re
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
                meeting_count INTEGER DEFAULT 0,
                transcript_count INTEGER DEFAULT 0,
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
        # Update the meeting count and transcript count for the authority
        conn.execute(
            """
            UPDATE authorities
            SET meeting_count = (
                SELECT COUNT(*) FROM meetings WHERE authority = id
            ),
            transcript_count = (
                SELECT COUNT(*) FROM transcripts_fts WHERE uid IN (
                    SELECT uid FROM meetings WHERE authority = id
                )
            )
            WHERE id = ?
            """,
            (authority,),
        )


def get_meeting_ids(authority: str) -> list[str]:
    """
    Get a list of meeting IDs for a given authority.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT uid FROM meetings WHERE authority = ?
        """,
            (authority,),
        )
        meeting_ids = [row[0] for row in cursor.fetchall()]
    return meeting_ids


def get_meeting_ids_with_transcripts(authority: str) -> list[str]:
    """
    Get a list of meeting IDs for a given authority that have transcripts.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT uid FROM meetings WHERE authority = ?
            AND uid IN (SELECT uid FROM transcripts_fts)
        """,
            (authority,),
        )
        meeting_ids = [row[0] for row in cursor.fetchall()]
    return meeting_ids


def get_authorities_and_transcript_counts() -> dict[str, int]:
    """
    Get a list of authorities and their corresponding number of available transcripts.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT id, transcript_count
            FROM authorities
            ORDER BY id ASC
        """
        )
        authorities = {row[0]: row[1] for row in cursor.fetchall()}
    return authorities

clean_query = re.compile(r'[^a-zA-Z0-9"]')
def sanitize_query(query: str) -> str:
    text = clean_query.sub(' ', query)
    if text.count('"') % 2 == 1:
        text = text.replace('"', '')
    return text

def build_count_and_query_string(
    query: str,
    authority: list[str] | None = None,
    startdate: str | None = None,
    enddate: str | None = None,
    sort_by: str = "relevance",
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[str, str, list[str], list[str]]:

    params = [sanitize_query(query)]

    # Search for the phrase in the transcripts using FTS
    count_string = """
        SELECT COUNT(*) 
        FROM transcripts_fts
        WHERE transcript MATCH ?
    """

    query_string = """
        SELECT uid, snippet(transcripts_fts, 1, '[', ']', '', 70) AS snippet, rank, transcript
        FROM transcripts_fts
        WHERE transcript MATCH ?
    """

    # Filter by authority if provided
    if authority:
        authority_filter = """
            AND uid IN (
                SELECT uid FROM meetings WHERE authority IN ({})
            )
        """.format(
            ",".join("?" for _ in authority)
        )

        query_string += authority_filter
        count_string += authority_filter
        params.extend(authority)

    # Filter by startdate and enddate if provided
    if startdate:
        startdate_filter = """
            AND uid IN (
                SELECT uid FROM meetings WHERE datetime >= ?
            )
        """
        query_string += startdate_filter
        count_string += startdate_filter
        params.append(startdate)

    if enddate:
        enddate_filter = """
            AND uid IN (
                SELECT uid FROM meetings WHERE datetime <= ?
            )
        """
        query_string += enddate_filter
        count_string += enddate_filter
        params.append(enddate)

    # Sort results based on the sort_by parameter
    if sort_by == "date_asc":
        query_string += " ORDER BY (SELECT datetime FROM meetings WHERE meetings.uid = transcripts_fts.uid) ASC"
    elif sort_by == "date_desc":
        query_string += " ORDER BY (SELECT datetime FROM meetings WHERE meetings.uid = transcripts_fts.uid) DESC"
    else:  # Default to relevance
        query_string += " ORDER BY bm25(transcripts_fts)"

    # Add pagination if limit and offset are provided
    if limit is not None and offset is not None:
        query_string += " LIMIT ? OFFSET ?"
        query_params = params + [str(limit), str(offset)]
    else:
        query_params = params

    return count_string, query_string, params, query_params


def search_meetings(
    query: str,
    authority: list[str] | None = None,
    startdate: str | None = None,
    enddate: str | None = None,
    sort_by: str = "relevance",
    limit: int | None = None,
    offset: int = 0,
) -> dict:
    """
    Search for meetings based on the query string.
    Returns a dict with 'results' (list of meeting details) and 'total' (total number of matches).
    """

    count_string, query_string, count_params, query_params = (
        build_count_and_query_string(
            query=query,
            authority=authority,
            startdate=startdate,
            enddate=enddate,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )
    )

    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute(count_string, count_params).fetchone()[0]
        results = conn.execute(query_string, query_params).fetchall()

        # To efficiently find the closest timestamp to the match snippet we remove the
        # brackets from the snippet and find the offset in the transcript

        offset_requests = []
        snippet_map = {}
        rank_map = {}

        for uid, snippet, rank, transcript in results:
            snippet_cleaned = snippet.replace("[", "").replace("]", "")
            offset_val = transcript.find(snippet_cleaned)
            offset_requests.append((uid, offset_val))
            snippet_map[(uid, offset_val)] = snippet
            rank_map[(uid, offset_val)] = rank

        formatted_results = []

        if offset_requests:
            # For each (uid, offset_val), fetch the closest offset <= offset_val and join meeting metadata
            batch_results = []
            for uid, offset_val in offset_requests:
                row = conn.execute(
                    """
                    SELECT o.start_time, o.start_time_seconds, m.title, m.datetime, m.unixtime, m.link, m.authority
                    FROM offsets o
                    JOIN meetings m ON o.uid = m.uid
                    WHERE o.uid = ? AND o.offset <= ?
                    ORDER BY o.offset DESC
                    LIMIT 1
                    """,
                    (uid, offset_val),
                ).fetchone()
                if row:
                    batch_results.append((uid, offset_val, *row))

            for (
                uid,
                offset_val,
                start_time,
                start_time_seconds,
                title,
                datetime,
                unixtime,
                link,
                authority,
            ) in batch_results:
                meeting_link = f"{link}/start_time/{1000*start_time_seconds}"
                formatted_results.append(
                    {
                        "title": title,
                        "datetime": datetime,
                        "unixtime": unixtime,
                        "snippet": snippet_map[(uid, offset_val)],
                        "start_time": start_time,
                        "rank": rank_map[(uid, offset_val)],
                        "link": meeting_link,
                        "authority": authority,
                    }
                )

        return {"results": formatted_results, "total": total}


def get_available_authorities_and_providers() -> list[tuple[str, str, dict | None]]:
    """
    Get a list of available authorities, corresponding providers, and configs.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT authorities.id, providers.id, providers.config
            FROM authorities
            JOIN providers ON authorities.provider = providers.id
        """
        )
        authorities_providers_configs = cursor.fetchall()
    return authorities_providers_configs


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
