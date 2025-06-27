from datetime import datetime, timedelta, timezone
from typing import Annotated
from contextlib import asynccontextmanager
import getpass

import jwt
import bcrypt
from fastapi import Depends, APIRouter, HTTPException, status, FastAPI
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel
import sqlite3
from os import getenv

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
DB_PATH = "../data/users.db"

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler to create the user database when the app starts.
    """
    # Lines here will run when the app starts
    create_user_database()
    yield
    # Lines here will run when the app stops


router = APIRouter(lifespan=lifespan)

def create_admin_user() -> tuple[str, str, str, bytes]:

    # Check environment variables for admin user details
    username = getenv("ADMIN_USERNAME","")
    password = getenv("ADMIN_PASSWORD","")
    full_name = getenv("ADMIN_FULL_NAME","")
    email = getenv("ADMIN_EMAIL","")

    if username and password:
        hashed_password = get_password_hash(password)
        return username, full_name, email, hashed_password

    # If environment variables are not set, prompt for input
    print("No admin user details found in environment variables.")
    print("Please enter the details for the admin user.")
    username = input("Enter a username: ")
    full_name = input("Enter full name: ")
    email = input("Enter email: ")
    password = getpass.getpass("Enter password: ")
    hashed_password = get_password_hash(password)

    if not username or not password:
        raise ValueError("Username and password are required.")
    
    return username, full_name, email, hashed_password


def create_user_database()-> None:
    # Create tables
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                full_name TEXT,
                email TEXT UNIQUE,
                hashed_password BLOB,
                disabled BOOLEAN DEFAULT FALSE
            )
        ''')
        # If the table is empty, create a user
        cursor = conn.execute('SELECT COUNT(*) FROM users')
        if cursor.fetchone()[0] == 0:
            username, full_name, email, hashed_password = create_admin_user()
            conn.execute('''
                INSERT INTO users (username, full_name, email, hashed_password)
                VALUES (?, ?, ?, ?)
            ''', (username, full_name, email, hashed_password))

class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None


class User(BaseModel):
    username: str
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = None


class UserInDB(User):
    hashed_password: str


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")


def verify_password(plain_password, hashed_password) -> bool:
    plain_utf8 = plain_password.encode('utf-8')
    hashed_utf8 = hashed_password.encode('utf-8') 
    return bcrypt.checkpw(plain_utf8, hashed_utf8)


def get_password_hash(password:str) -> bytes:
    password_utf8 = password.encode('utf-8')
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password_utf8, salt)


def get_user(username: str) -> UserInDB | None:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute('''
                SELECT username, full_name, email, hashed_password, disabled FROM users
                WHERE username = ?
            ''', (username,))

        user = cursor.fetchone()

    if not user:
        return None
    
    username, full_name, email, hashed_password, disabled = user

    return UserInDB(
        username=username,
        full_name=full_name,
        email=email,
        hashed_password=hashed_password,
        disabled=disabled
    )


def authenticate_user(username: str, password: str) -> UserInDB | None:
    user = get_user(username)
    if not user:
        return
    if not verify_password(password, user.hashed_password):
        return 
    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]) -> UserInDB:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except InvalidTokenError:
        raise credentials_exception
    if token_data.username is None:
        raise credentials_exception
    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


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
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")


@router.get("/auth/me/", tags=["auth"] ,response_model=User)
async def read_users_me(
    current_user: Annotated[User, Depends(get_current_active_user)],
) -> User:
    return current_user


@router.get("/auth/me/items/", tags=["auth"])
async def read_own_items(
    current_user: Annotated[User, Depends(get_current_active_user)],
) -> list[dict]:
    return [{"item_id": "Foo", "owner": current_user.username}]