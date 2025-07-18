# Standard library
from contextlib import asynccontextmanager
from os import urandom
from pathlib import Path
from typing import Annotated
from datetime import datetime, timedelta, timezone

# Third-party libraries
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import jwt
from jwt.exceptions import InvalidTokenError
from passlib.hash import argon2

# Local application imports
from api.models.users import User, UserInDB
from api.models.auth import Token
from api.db.users import get_user

# Constants
SECRET_KEY_PATH = (Path(__file__).parent.parent / "data" / "jwt_secret.key").resolve()
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# API
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler.
    """
    # Lines here will run when the app starts
    create_jwt_secret()
    yield
    # Lines here will run when the app stops


router = APIRouter(lifespan=lifespan)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")


def create_jwt_secret() -> None:
    """
    Create a new token signing key when the app starts.
    Existing tokens are invalidated and users will need to log in again.
    """
    signing_key = urandom(32)  # Generate a new random secret key
    with open(SECRET_KEY_PATH, "wb") as key_file:
        key_file.write(signing_key)

    return


def get_secret_key() -> bytes:
    """
    Get the secret key for signing JWT tokens.
    If the key file does not exist, create a new one.
    """
    try:
        with open(SECRET_KEY_PATH, "rb") as key_file:
            return key_file.read()
    except FileNotFoundError:
        create_jwt_secret()
        with open(SECRET_KEY_PATH, "rb") as key_file:
            return key_file.read()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return argon2.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return argon2.hash(password)


def authenticate_user(username: str, password: str) -> UserInDB | None:
    user = get_user(username)
    if not user:
        return
    if not verify_password(password, user.hashed_password):
        return
    return user


def create_access_token(data: dict):
    to_encode = data.copy()

    expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = datetime.now(timezone.utc) + expires_delta

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, get_secret_key(), algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
) -> UserInDB:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, get_secret_key(), algorithms=[ALGORITHM])
    except InvalidTokenError:
        raise credentials_exception
    username = payload.get("sub")
    if not isinstance(username, str):
        raise credentials_exception
    user = get_user(username=username)
    if user is None:
        raise credentials_exception
    return user


async def get_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


async def get_admin_user(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    if not current_user.admin:
        raise HTTPException(status_code=400, detail="Not an admin")
    return current_user


# Annotated types for adding authentication dependencies
active_user = Annotated[User, Depends(get_active_user)]
admin_user = Annotated[User, Depends(get_admin_user)]


# Endpoints
@router.post("/auth/token", tags=["auth"])
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.username})
    return Token(access_token=access_token, token_type="bearer")
