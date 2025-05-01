from flask import Flask, request, jsonify
from flask_cors import CORS  # Added import for CORS
import sqlite3

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Explicitly allow all origins
DB_PATH = '../data/birmingham_council_meetings.db'

def query_db(query, args=(), one=False):
    """Helper function to query the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@app.route('/search', methods=['GET'])
def search_meetings():
    """Endpoint to search meeting transcript."""
    search_query = request.args.get('query', '')
    if not search_query:
        return jsonify({"error": "Query parameter is required"}), 400
    
    conn = sqlite3.connect(DB_PATH)

    # Search for the phrase in the transcripts using FTS
    query = '''
        SELECT uid, snippet(transcripts_fts, 1, '[', ']', '', 70) AS snippet, rank, transcript
        FROM transcripts_fts
        WHERE transcript MATCH ?
        ORDER BY bm25(transcripts_fts)
    '''

    results = conn.execute(query, (search_query,)).fetchall()

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
            SELECT title, date, link FROM meetings
            WHERE uid = ?
        ''', (uid,))
        meeting_title, meeting_date, meeting_link = meeting_cursor.fetchone()
        meeting_link = f"{meeting_link}/start_time/{1000*start_time_seconds}"

        formatted_results.append({
            'title': meeting_title,
            'date': meeting_date,
            'snippet': snippet,
            'start_time': start_time,
            'rank': rank,
            'link': meeting_link
        })
        
    conn.close()

    return jsonify(formatted_results)

if __name__ == '__main__':
    app.run(debug=True)