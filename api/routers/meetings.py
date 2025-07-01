# Standard library
from typing import Union

# Third-party libraries
from contextlib import asynccontextmanager
from fastapi import APIRouter, FastAPI, Query
from fastapi.responses import JSONResponse

# Local application imports
from api.db import meetings


# API
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler.
    """
    # Lines here will run when the app starts
    yield
    # Lines here will run when the app stops


router = APIRouter(lifespan=lifespan)


@router.get("/search", tags=["meetings"])
async def search_meetings(
    query: str,
    authority: Union[list[str], None] = Query(None),
    startdate: Union[str, None] = None,
    enddate: Union[str, None] = None,
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
    )

    return JSONResponse(content=results)


@router.get("/transcript_counts_by_authority", tags=["meetings"])
async def available_authorities() -> JSONResponse:
    """
    Endpoint to get available authorities and their corresponding number of available transcripts.
    """

    authorities_and_counts = meetings.get_authorities_and_transcript_counts()
    return JSONResponse(content=authorities_and_counts)
