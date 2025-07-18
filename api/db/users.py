# Standard library
from pathlib import Path
from typing import Callable

# Third-party libraries
import sqlite3

# Local application imports
from api.models.users import UserInDB

# Constants
DB_PATH = (Path(__file__).parent.parent / "data" / "users.db").resolve()


def get_user(username: str) -> UserInDB | None:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
                SELECT username, full_name, email, hashed_password, disabled, admin FROM users
                WHERE username = ?
            """,
            (username,),
        )

        user = cursor.fetchone()

    if not user:
        return None

    username, full_name, email, hashed_password, disabled, admin = user

    return UserInDB(
        username=username,
        full_name=full_name,
        email=email,
        hashed_password=hashed_password,
        disabled=disabled,
        admin=admin,
    )


def add_user_to_db(user: UserInDB) -> None:

    username = user.username
    full_name = user.full_name
    email = user.email
    hashed_password = user.hashed_password
    admin = user.admin

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
                INSERT INTO users (username, full_name, email, hashed_password, admin)
                VALUES (?, ?, ?, ?, ?)
            """,
            (username, full_name, email, hashed_password, admin),
        )


def create_user_database(create_admin_user: Callable[[], UserInDB]) -> None:
    # Create tables
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                full_name TEXT,
                email TEXT UNIQUE,
                hashed_password BLOB,
                disabled BOOLEAN DEFAULT FALSE,
                admin BOOLEAN DEFAULT FALSE
            )
        """
        )
        # If the table is empty, create a user
        cursor = conn.execute("SELECT COUNT(*) FROM users")
        if cursor.fetchone()[0] == 0:
            add_user_to_db(create_admin_user())
