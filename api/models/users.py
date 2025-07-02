from pydantic import BaseModel


class User(BaseModel):
    username: str
    email: str | None = None
    full_name: str | None = None
    disabled: bool = False
    admin: bool = False


class UserInDB(User):
    hashed_password: str
