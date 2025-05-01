# Gather metadata and subtitles from council meetings using an RSS feed
# Parse subtitle vtt files

import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xmltodict
import re
import sqlite3

transcript_url = "https://cl-assets.public-i.tv/birmingham/subtitles/birmingham_{uid}_en_GB.vtt"
rss_url = "https://birmingham.public-i.tv/core/data/18549/archived/1/agenda/1"

def get_rss_feed(url):
    """Fetch the RSS feed from the given URL and return the parsed data."""
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch RSS feed: {response.status_code}")

    return xmltodict.parse(response.content)

def get_transcript(url):
    """Fetch the transcript from the given URL and return the parsed data."""
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch transcript at {url}: {response.status_code}")
        return None

    return response.text

def parse_vtt(vtt_text):
    """Parse the VTT text and return a list of tuples with start time, end
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

def parse_item_from_public_i(full_item):
    item = {} 
    item['title'] = full_item.get('title')
    item['description'] = full_item.get('description')
    item['tags'] = full_item.get('pi:tags')
    item['date'] = full_item.get('pi:liveDate')
    item['link'] = full_item.get('guid')
    item['agenda'] = [
            {'id': agenda_item.get('pi:agenda_id'),
             'text': agenda_item.get('pi:agenda_text'),
             'time': agenda_item.get('pi:agenda_time'),
            } for agenda_item in (full_item.get('pi:agenda', {}) or {}).get('pi:agenda_item', [{}])
         ]
    item['uid'] = full_item.get('pi:activity')
    if item['uid'] != item['link'].split('/')[-1]:
        print(f"UID {item['uid']} does not match link {item['link']}")

    item['transcript_url'] = transcript_url.format(uid=item['uid'])
    item['transcript'] = get_transcript(item['transcript_url'])
    if item['transcript']:
        item['parsed_transcript'] = parse_vtt(item['transcript'])
    else:
        item['parsed_transcript'] = None

    return item


## Get the list of meetings from the RSS feed
feed = get_rss_feed(rss_url)

## For every meeting in the feed, get the transcript and parse it
with ThreadPoolExecutor() as executor:
    results = list(tqdm(executor.map(parse_item_from_public_i, feed['rss']['channel']['item']), total=len(feed['rss']['channel']['item'])))

directory = {item['uid']: item for item in results}

## Load the data into a SQLite database
conn = sqlite3.connect('./data/birmingham_council_meetings.db')

# Create tables
with conn:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS meetings (
            uid TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            date TEXT,
            link TEXT
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

# Insert data into tables
with conn:
    for uid, item in directory.items():
        conn.execute('''
            INSERT OR IGNORE INTO meetings (uid, title, description, date, link)
            VALUES (?, ?, ?, ?, ?)
        ''', (uid, item['title'], item['description'], item['date'], item['link']))
        
        if item['parsed_transcript']:
            for start_time, end_time, text in item['parsed_transcript']:
                conn.execute('''
                    INSERT INTO transcripts (uid, transcript, title, description, start_time, end_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (uid, text, item['title'], item['description'], start_time, end_time))
            
        for agenda_item in item['agenda']:
            conn.execute('''
                INSERT OR IGNORE INTO agenda (uid, agenda_id, agenda_text, agenda_time)
                VALUES (?, ?, ?, ?)
            ''', (uid, agenda_item['id'], agenda_item['text'], agenda_item['time']))

        if item['parsed_transcript'] == None:
            continue
            
        full_transcript = " ".join(
            text for _, _, text in item['parsed_transcript'] 
        )

        offset = 0
        for start_time, end_time, text in item['parsed_transcript']:
            # Convert the start time from hh:mm:ss.sss to seconds
            start_time_seconds = sum(
                float(x) * 60 ** i for i, x in enumerate(reversed(start_time.split(":")))
            )//1

            conn.execute('''
                INSERT INTO offsets (uid, offset, start_time, start_time_seconds)
                VALUES (?, ?, ?, ?)
            ''', (uid, offset, start_time, start_time_seconds))
            offset += len(text) + 1  # Add 1 for the space between words

        conn.execute('''
            INSERT INTO transcripts_fts (uid, transcript)
            VALUES (?, ?)
        ''', (uid, full_transcript))

conn.close()