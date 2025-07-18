from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import auth, users, meetings

app = FastAPI()
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(meetings.router)

# Allow CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
