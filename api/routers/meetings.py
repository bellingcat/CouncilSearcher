# Standard library
from contextlib import asynccontextmanager
from typing import Literal, Union

# Third-party libraries
from fastapi import APIRouter, FastAPI, Query, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Local application imports
from api.db import meetings
from api.providers.provider import Provider
from api.routers.auth import active_user, admin_user
import io
import csv

scheduler = AsyncIOScheduler()


# API
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler.
    """
    # Lines here will run when the app starts
    meetings.create_database()
    scheduler.start()
    yield
    # Lines here will run when the app stops
    scheduler.shutdown()


router = APIRouter(lifespan=lifespan)


@router.get("/meetings/search", tags=["meetings"])
async def search_meetings(
    query: str,
    authority: Union[list[str], None] = Query(None),
    startdate: Union[str, None] = None,
    enddate: Union[str, None] = None,
    sort_by: Literal["relevance", "date_asc", "date_desc"] = "relevance",
    limit: Union[int, None] = None,
    offset: int = 0,
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
        return JSONResponse(
            content={"error": "Query parameter is required"}, status_code=400
        )

    results = meetings.search_meetings(
        query=query,
        authority=authority,
        startdate=startdate,
        enddate=enddate,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
    )

    return JSONResponse(content=results)


@router.get("/meetings/transcript_counts_by_authority", tags=["meetings"])
async def available_authorities() -> JSONResponse:
    """
    Endpoint to get available authorities and their corresponding number of available transcripts.
    """

    authorities_and_counts = meetings.get_authorities_and_transcript_counts()
    return JSONResponse(content=authorities_and_counts)


@router.post("/meetings/add_authority", tags=["meetings"])
async def add_authority(
    current_user: admin_user,
    authority: str,
    nice_name: str,
    provider: str,
) -> JSONResponse:
    """
    Add a new authority to the database.

    Parameters:
    - authority: The name of the authority.
    - provider: The name of the provider.
    - nice_name: A user-friendly name for the authority.

    Returns:
    - JSON response indicating success or failure.
    """

    if not authority or not provider:
        return JSONResponse(
            content={"error": "Authority and provider are required"},
            status_code=400,
        )

    try:
        meetings.add_authority(authority, provider, nice_name)
        return JSONResponse(content={"message": "Authority added successfully"})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/meetings/add_provider", tags=["meetings"])
async def add_provider(
    current_user: admin_user,
    provider: str,
    config: dict | None = None,
) -> JSONResponse:
    """
    Add a new provider to the database.

    Parameters:
    - provider: The name of the provider.
    - config: Optional; configuration for the provider.

    Returns:
    - JSON response indicating success or failure.
    """

    if not provider:
        return JSONResponse(
            content={"error": "Provider is required"},
            status_code=400,
        )

    try:
        meetings.add_provider(provider, config)
        return JSONResponse(content={"message": "Provider added successfully"})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# Load all meetings weekly and load new meetings daily
@scheduler.scheduled_job(
    "cron", day_of_week="sun", hour=2, minute=0, kwargs={"update": "all"}
)
@scheduler.scheduled_job("cron", hour=0, minute=0, kwargs={"update": "new"})
def load_meetings(update: Literal["all", "new", "missing"] = "all") -> None:

    authorities_providers_configs = meetings.get_available_authorities_and_providers()

    for authority, provider_name, config in authorities_providers_configs:
        provider = Provider.create(provider_name, authority, config)
        index = provider.build_index()
        if update == "all":
            # No filtering, load all meetings
            pass
        elif update == "new":
            # Filter the index to only include new meetings
            existing_meetings = meetings.get_meeting_ids(authority)
            index = [
                meeting
                for meeting in index
                if meeting.get("uid") not in existing_meetings
            ]
        elif update == "missing":
            # Filter the index to meetings that are missing transcripts
            meetings_with_transcripts = meetings.get_meeting_ids_with_transcripts(
                authority
            )
            index = [
                meeting
                for meeting in index
                if meeting.get("uid") not in meetings_with_transcripts
            ]
        else:
            raise ValueError("Invalid update type. Use 'all', 'new', or 'missing'.")

        meetings.add_meetings_to_db(
            authority=authority, meetings=provider.get_meetings(index)
        )


@router.post("/meetings/load", tags=["meetings"])
async def trigger_load_meetings(
    current_user: admin_user,
    background_tasks: BackgroundTasks,
    update: Literal["all", "new", "missing"] = "all",
) -> JSONResponse:
    """
    Load meetings from all authorities into the database.
    This function is intended to be called periodically
    """

    background_tasks.add_task(load_meetings, update)

    return JSONResponse(content={"message": "Job submitted."})


@router.get("/meetings/download_transcript/{uid}", tags=["meetings"])
async def download_transcript(uid: str) -> StreamingResponse:
    """
    Download the full transcript for a specified meeting as .txt file
    """
    transcript_data = meetings.get_full_transcript(uid)
    
    if not transcript_data:
        return JSONResponse(
            content={"error": "Transcript not found"},
            status_code=404
        )
    
    # Add transcript info to header
    output = io.StringIO()
    output.write(f"Meeting: {transcript_data['title']}\n")
    output.write(f"Authority: {transcript_data['authority'].title()}\n")
    output.write(f"Date: {transcript_data['datetime']}\n")
    output.write(f"\n{'='*80}\n\n")
    output.write(transcript_data['transcript'])
    
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/plain"
    )