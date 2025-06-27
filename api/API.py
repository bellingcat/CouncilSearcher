from typing import Union

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import sqlite3

app = FastAPI()

# Allow CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Database path
DB_PATH = '../data/council_meetings.db'


@app.get("/search")
async def search_meetings(
    query: str,
    authority: Union[list[str], None] = Query(None),
    startdate: Union[str, None] = None,
    enddate: Union[str, None] = None
) -> JSONResponse:
    """
    Endpoint to search meeting transcript.
    
    Parameters:
    - query: The search phrase.
    - authority: Optional; filter by authority name.
    - startdate: Optional; filter by start date (YYYY-MM-DD).
    - enddate: Optional; filter by end date (YYYY-MM-DD).
    
    Returns:
    - JSON response with search results.
    """

    if not query:
        return JSONResponse(content={"error": "Query parameter is required"}, status_code=400)
    
    # Search for the phrase in the transcripts using FTS
    query_string = '''
        SELECT uid, snippet(transcripts_fts, 1, '[', ']', '', 70) AS snippet, rank, transcript
        FROM transcripts_fts
        WHERE transcript MATCH ?
    '''
    params = [query]

    # Filter by authority if provided
    if authority:
        query_string += '''
            AND uid IN (
                SELECT uid FROM meetings WHERE authority IN ({})
            )
        '''.format(','.join('?' for _ in authority))
        params.extend(authority)

    # Filter by startdate and enddate if provided
    if startdate:
        query_string += '''
            AND uid IN (
                SELECT uid FROM meetings WHERE datetime >= ?
            )
        '''
        params.append(startdate)

    if enddate:
        query_string += '''
            AND uid IN (
                SELECT uid FROM meetings WHERE datetime <= ?
            )
        '''
        params.append(enddate)

    query_string += ' ORDER BY bm25(transcripts_fts)'

    with sqlite3.connect(DB_PATH) as conn:
        results = conn.execute(query_string, params).fetchall()
    
        formatted_results = []
        for result in results:
            # Calculate the start time from the offset
            uid, snippet, rank, transcript = result
            # Use the matched snippet to find the offset in the full transcript
            snippet_cleaned = snippet.replace('[', '').replace(']', '')

            offset = transcript.find(snippet_cleaned)

            # Get the largest offset less than the current offset
            cursor = conn.execute('''
                SELECT start_time, start_time_seconds FROM offsets
                WHERE uid = ? AND offset <= ? ORDER BY offset DESC LIMIT 1
            ''', (uid, offset))

            start_time, start_time_seconds = cursor.fetchone()

            # Fetch the link from the meetings table matching the uid
            meeting_cursor = conn.execute('''
                SELECT title, datetime, unixtime, link, authority FROM meetings
                WHERE uid = ?
            ''', (uid,))
            meeting_title, datetime, unixtime, meeting_link, authority_result = meeting_cursor.fetchone()
            meeting_link = f"{meeting_link}/start_time/{1000*start_time_seconds}"

            formatted_results.append({
                'title': meeting_title,
                'datetime': datetime,
                'unixtime': unixtime,
                'snippet': snippet,
                'start_time': start_time,
                'rank': rank,
                'link': meeting_link,
                'authority': authority_result
            })

    return JSONResponse(content=formatted_results)

@app.get("/transcript_counts_by_authority")
async def available_authorities() -> JSONResponse:
    """
    Endpoint to get available authorities and their transcript counts.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute('''
            SELECT authority, COUNT(*) as transcript_count
            FROM authorities
            JOIN meetings ON authorities.id = meetings.authority
            WHERE meetings.uid IN (
                SELECT uid FROM transcripts_fts
            )
            GROUP BY authority
        ''')
        authorities = {row[0]: row[1] for row in cursor.fetchall()}

    return JSONResponse(content=authorities)

def get_transcript_and_meeting_counts(
        authority: str
    ) -> tuple[int, int]:
    
    with sqlite3.connect(DB_PATH) as conn:
        # Compare number of transcripts_fts and meetings
        cursor = conn.execute('''
            SELECT COUNT(*) FROM transcripts_fts WHERE uid IN (
                SELECT uid FROM meetings WHERE authority = ?
            )
        ''', (authority,))
        
        transcripts_count = cursor.fetchone()[0]

        cursor = conn.execute('''
            SELECT COUNT(*) FROM meetings WHERE authority = ?
        ''', (authority,))

        meetings_count = cursor.fetchone()[0]

    return transcripts_count, meetings_count

def create_database(db_path: str)-> None:
    # Create tables
    with sqlite3.connect(db_path) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS authorities (
                id TEXT PRIMARY KEY
            )
        ''')
        conn.execute('''
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
        ''')
        conn.execute('''
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
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS agenda (
                uid TEXT,
                agenda_id TEXT,
                agenda_text TEXT,
                agenda_time TEXT,
                PRIMARY KEY (uid, agenda_id),
                FOREIGN KEY (uid) REFERENCES meetings(uid)
            )
        ''')
        conn.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS transcripts_fts USING fts5(
                uid,
                transcript,
                tokenize='porter'
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS offsets (
                uid TEXT,
                offset INTEGER,
                start_time TEXT,
                start_time_seconds INTEGER,
                PRIMARY KEY (uid, offset),
                FOREIGN KEY (uid) REFERENCES meetings(uid)
            )
        ''')
    return     
