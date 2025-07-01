# Standard library
from contextlib import asynccontextmanager
import getpass
from os import getenv

# Third-party libraries
from fastapi import APIRouter, FastAPI

# Local application imports
from api.models.users import User, UserInDB
from api.db.users import add_user_to_db, create_user_database
from api.routers.auth import active_user, admin_user, get_password_hash


# API
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler.
    """
    # Lines here will run when the app starts
    create_user_database(create_admin_user)
    yield
    # Lines here will run when the app stops


router = APIRouter(lifespan=lifespan)


def create_admin_user() -> UserInDB:

    # Check environment variables for admin user details
    username = getenv("ADMIN_USERNAME", "")
    password = getenv("ADMIN_PASSWORD", "")
    full_name = getenv("ADMIN_FULL_NAME", "")
    email = getenv("ADMIN_EMAIL", "")

    if not (username and password):
        # If environment variables are not set, prompt for input
        print("No admin user details found in environment variables.")
        print("Please enter the details for the admin user.")
        username = input("Enter a username: ")
        full_name = input("Enter full name: ")
        email = input("Enter email: ")
        password = getpass.getpass("Enter password: ")

    if not username or not password:
        raise ValueError("Username and password are required.")

    hashed_password = get_password_hash(password)
    return UserInDB(
        username=username,
        full_name=full_name,
        email=email,
        hashed_password=hashed_password,
        disabled=False,
        admin=True,
    )


# Endpoints
@router.get("/users/me", tags=["users"], response_model=User)
async def read_users_me(
    current_user: active_user,
) -> User:
    return current_user


@router.post("/users/create", tags=["users"])
async def create_user(
    current_user: admin_user,
    username: str,
    password: str,
    full_name: str | None = None,
    email: str | None = None,
    disabled: bool = False,
    admin: bool = False,
) -> None:

    hashed_password = get_password_hash(password)
    add_user_to_db(
        UserInDB(
            username=username,
            full_name=full_name,
            email=email,
            hashed_password=hashed_password,
            disabled=disabled,
            admin=admin,
        )
    )
    return
