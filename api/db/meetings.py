import sqlite3

from api.models.meetings import MeetingItem

DB_PATH = "../data/council_meetings.db"


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
