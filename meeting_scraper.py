# Gather metadata and subtitles from council meetings using an RSS feed
# Parse subtitle vtt files

import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xmltodict
import re
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

authorities = [
    "aberdeen",
    "bedford",
    "belfastcity",
    "bexley",
    "birmingham",
    "bolton",
    "bradford",
    "braintree",
    "brent",
    "buckinghamshire",
    "calderdale",
    "cardiff",
    "centralbedfordshire",
    "cherwell",
    "cheshirewestandchester",
    "cotswold",
    "coventry",
    "denbighshire",
    "derrycityandstrabanedistrict",
    "dlrcoco",
    "donegalcoco",
    "dpea",
    "dublincity",
    "eastlothian",
    "eastrenfrewshire",
    "eastsussex",
    "edinburgh",
    "elmbridge",
    "eppingforestdc",
    "fingalcoco",
    "flintshire",
    "folkestone-hythe",
    "glasgow",
    "gloucestershire",
    "guildford",
    "gwynedd",
    "harrow",
    "hertsmere",
    "highland",
    "islington",
    "kent",
    "kingston",
    "knowsley",
    "leeds",
    "leicester",
    "lewes-eastbourne",
    "lewisham",
    "manchester",
    "midlothian",
    "north-ayrshire",
    "northlanarkshire",
    "pembrokeshire",
    "powys",
    "rctcbc",
    "reading",
    "renfrewshire",
    "richmond",
    "roscommon",
    "rother",
    "rotherham",
    "royalgreenwich",
    "sandwell",
    "sdcc",
    "sheffield",
    "solihull",
    "southdowns",
    "southend",
    "southglos",
    "southkesteven",
    "staffsmoorlands",
    "stalbans",
    "statesassembly",
    "sthelens",
    "stirling",
    "stockport",
    "stoke",
    "surreycc",
    "tandridge",
    "teignbridge",
    "tewkesbury",
    "thurrock",
    "torfaen",
    "towerhamlets",
    "wandsworth",
    "warrington",
    "wealden",
    "welhat",
    "westsussex",
    "wirral",
    "woking",
    "wolverhampton",
    "wrexham",
    "wyreforestdc",
    "ynysmon"
    ]

db_path = f"./data/council_meetings.db"

def resolve_urls_for_authority(authority):
    # use the magic_rss page to get the authority id (termed ds_id in the page)
    magic_rss = f"https://{authority}.public-i.tv/core/portal/magic_rss"
    print(f"Getting magic RSS feed for authority {authority}: {magic_rss}")
    response = requests.get(magic_rss)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch magic RSS feed: {response.status_code}")

    # Use BeautifulSoup to parse the HTML and extract the ds_id
    soup = BeautifulSoup(response.content, 'html.parser')
    ds_id_input = soup.find('input', {'name': 'ds_id'})
    authority_id = ds_id_input['value'] 
    print(f"Getting data for authority {authority} with id {authority_id}")

    # Construct urls for transcripts and database
    transcript_url = f"https://cl-assets.public-i.tv/{authority}/subtitles/{authority}_" + "{uid}_en_GB.vtt"
    rss_url = f"https://{authority}.public-i.tv/core/data/{authority_id}/archived/1/agenda/1"
    return transcript_url, rss_url

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
    item['date'] = full_item.get('pi:liveDate') # Format is Wed, 30 Apr 2025 19:30:00 +0100

    # Convert the date to a Unix timestamp
    item['unixtime'] = int(datetime.strptime(item['date'], "%a, %d %b %Y %H:%M:%S %z").timestamp()) if item['date'] else None
    item['datetime'] = datetime.strptime(item['date'], "%a, %d %b %Y %H:%M:%S %z").isoformat(" ") if item['date'] else None

    item['link'] = full_item.get('guid')

    agenda_items = (full_item.get('pi:agenda', {}) or {}).get('pi:agenda_item', [])
    if isinstance(agenda_items, dict):
        agenda_items = [agenda_items]
        
    item['agenda'] = [
            {'id': agenda_item.get('pi:agenda_id'),
             'text': agenda_item.get('pi:agenda_text'),
             'time': agenda_item.get('pi:agenda_time'),
            } for agenda_item in agenda_items
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

def create_database(db_path):
    conn = sqlite3.connect(db_path)

    # Create tables
    with conn:
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

def insert_data(directory):
    ## Load the data into a SQLite database
    conn = sqlite3.connect(db_path)            

    # Insert data into tables
    with conn:
        # Insert authority data
        conn.execute('''
            INSERT OR IGNORE INTO authorities (id)
            VALUES (?)
        ''', (authority,))
        for uid, item in directory.items():
            conn.execute('''
                INSERT OR IGNORE INTO meetings (uid, authority, title, description, datetime, unixtime, link)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (uid, authority, item['title'], item['description'], item['datetime'], item['unixtime'], item['link']))
            
            if item['parsed_transcript']:
                for start_time, end_time, text in item['parsed_transcript']:
                    conn.execute('''
                        INSERT OR IGNORE INTO transcripts (uid, transcript, title, description, start_time, end_time)
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
                    INSERT OR IGNORE INTO offsets (uid, offset, start_time, start_time_seconds)
                    VALUES (?, ?, ?, ?)
                ''', (uid, offset, start_time, start_time_seconds))
                offset += len(text) + 1  # Add 1 for the space between words

            # Only insert the full transcript if it doesn't already exist 
            # As virtual table doesn't support UNIQUE constraint
            conn.execute('''
                INSERT INTO transcripts_fts (uid, transcript)
                SELECT ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM transcripts_fts WHERE uid = ?
                )
            ''', (uid, full_transcript, uid))

def get_transcript_and_meeting_counts(authority):
    ## Load the data into a SQLite database
    conn = sqlite3.connect(db_path)    

    with conn:
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

## Create the database if it doesn't exist
create_database(db_path)

for authority in tqdm(authorities):

    ## Resolve the URLs for the authority
    transcript_url, rss_url = resolve_urls_for_authority(authority)

    ## Get the list of meetings from the RSS feed
    feed = get_rss_feed(rss_url)

    ## For every meeting in the feed, get the transcript and parse it
    items = feed['rss']['channel'].get('item', [])
    if not items:
        print(f"No items found in RSS feed for authority {authority}. Skipping...")
        continue

    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(parse_item_from_public_i, items), total=len(items)))

    directory = {item['uid']: item for item in results}

    ## Insert the data into the database
    insert_data(directory)

    ## Get the counts of transcripts and meetings
    transcripts_count, meetings_count = get_transcript_and_meeting_counts(authority)
    print(f"{authority} has transcripts for {transcripts_count} meetings out of {meetings_count}.")
