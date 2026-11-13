"""
Auth service: the one place that knows users and passwords.

- On first start it creates a default administrator (admin / admin). Change that
  password: the console asks for it at the first sign-in.
- Clients sign in with HTTP Basic (`curl -u admin:admin ...`) or with a token
  from POST /login (`Authorization: Bearer <token>`).
- The producer, consumer and console do not check passwords themselves. They
  pass the Authorization header to GET /verify and remember the answer for a
  few seconds.

Users are stored in ZooKeeper under /auth/users, passwords as salted PBKDF2 hashes.
"""
import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from typing import Optional

from fastapi import Body, Depends, FastAPI, Header, HTTPException
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NoNodeError, NodeExistsError

ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
# The administrator created when there are no users yet
DEFAULT_USER = os.getenv("AUTH_DEFAULT_USER", "admin")
DEFAULT_PASSWORD = os.getenv("AUTH_DEFAULT_PASSWORD", "admin")
# How long a token from /login stays valid
TOKEN_HOURS = float(os.getenv("AUTH_TOKEN_HOURS", "12"))
# Work factor of the password hash (higher = slower to guess, slower to check)
HASH_ITERATIONS = int(os.getenv("AUTH_HASH_ITERATIONS", "200000"))
MIN_PASSWORD_LENGTH = int(os.getenv("AUTH_MIN_PASSWORD_LENGTH", "8"))
# After this many wrong passwords in a row a user must wait LOCKOUT_SECONDS
MAX_FAILED_LOGINS = int(os.getenv("AUTH_MAX_FAILED_LOGINS", "5"))
LOCKOUT_SECONDS = float(os.getenv("AUTH_LOCKOUT_SECONDS", "60"))

USERS_PATH = "/auth/users"
SECRET_PATH = "/auth/secret"
USERNAME_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")
ROLES = ("admin", "user")   # admin: everything. user: publish, read, view the console.

zk = KazooClient(hosts=ZOOKEEPER_HOST)
zk.start()
app = FastAPI(title="Pub-Sub Auth")


# --- passwords -------------------------------------------------------------------

def hash_password(password: str, salt: Optional[str] = None) -> dict:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), HASH_ITERATIONS)
    return {"salt": salt, "hash": digest.hex(), "iterations": HASH_ITERATIONS}


# Checking a password is deliberately slow, so correct ones are remembered until
# the stored hash changes. The key includes that hash: a new password misses.
_checked = {}


def password_matches(user: dict, password: str) -> bool:
    key = hashlib.sha256(f"{user['hash']}:{password}".encode("utf-8")).hexdigest()
    if key in _checked:
        return True
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(user["salt"]), int(user.get("iterations", HASH_ITERATIONS))
    )
    if not hmac.compare_digest(digest.hex(), user["hash"]):
        return False
    if len(_checked) > 5000:
        _checked.clear()
    _checked[key] = True
    return True


def check_new_password(password: str) -> None:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise HTTPException(status_code=400, detail=f"The password needs at least {MIN_PASSWORD_LENGTH} characters")


# --- users -----------------------------------------------------------------------

def user_path(username: str) -> str:
    if not USERNAME_RE.match(username or ""):
        raise HTTPException(status_code=400, detail="Usernames use 1-64 letters, digits and . _ @ -")
    return f"{USERS_PATH}/{username}"


def load_user(username: str) -> Optional[dict]:
    if not USERNAME_RE.match(username or ""):
        return None
    try:
        data, _ = zk.get(f"{USERS_PATH}/{username}")
    except NoNodeError:
        return None
    return json.loads(data.decode("utf-8"))


def save_new_user(username: str, password: str, role: str, must_change_password: bool) -> None:
    record = {
        "username": username,
        "role": role,
        "must_change_password": must_change_password,
        "password_version": 1,
        **hash_password(password),
    }
    zk.create(user_path(username), json.dumps(record).encode("utf-8"), makepath=True)


def set_password(username: str, password: str, must_change_password: bool) -> None:
    """Store a new password. Tokens issued for the old one stop working."""
    path = user_path(username)
    for _ in range(10):
        try:
            data, stat = zk.get(path)
        except NoNodeError:
            raise HTTPException(status_code=404, detail="User not found")
        record = json.loads(data.decode("utf-8"))
        record.update(hash_password(password))
        record["must_change_password"] = must_change_password
        record["password_version"] = int(record.get("password_version", 1)) + 1
        try:
            zk.set(path, json.dumps(record).encode("utf-8"), version=stat.version)
            return
        except BadVersionError:
            continue
    raise HTTPException(status_code=503, detail="The user keeps changing, try again")


def public(user: dict) -> dict:
    return {
        "username": user["username"],
        "role": user["role"],
        "must_change_password": bool(user.get("must_change_password")),
    }


def create_default_admin() -> None:
    zk.ensure_path(USERS_PATH)
    if not zk.get_children(USERS_PATH):
        try:
            save_new_user(DEFAULT_USER, DEFAULT_PASSWORD, "admin", must_change_password=True)
        except NodeExistsError:
            pass   # another auth instance created it first


def signing_secret() -> bytes:
    """Key that signs tokens. Generated once and shared by all auth instances."""
    try:
        zk.create(SECRET_PATH, secrets.token_hex(32).encode("utf-8"), makepath=True)
    except NodeExistsError:
        pass
    return zk.get(SECRET_PATH)[0]


create_default_admin()
SECRET = signing_secret()


# --- tokens ----------------------------------------------------------------------

def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def make_token(user: dict) -> tuple:
    expires_at = time.time() + TOKEN_HOURS * 3600
    payload = b64(json.dumps(
        {"sub": user["username"], "pv": user.get("password_version", 1), "exp": expires_at}
    ).encode("utf-8"))
    signature = b64(hmac.new(SECRET, payload.encode("ascii"), hashlib.sha256).digest())
    return f"{payload}.{signature}", expires_at


def user_for_token(token: str) -> Optional[dict]:
    try:
        payload, signature = token.split(".")
        expected = b64(hmac.new(SECRET, payload.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            return None
        claims = json.loads(unb64(payload))
    except (ValueError, binascii.Error, UnicodeError):
        return None
    if claims.get("exp", 0) < time.time():
        return None
    user = load_user(claims.get("sub", ""))
    if user is None or user.get("password_version", 1) != claims.get("pv"):
        return None   # user deleted, or the password changed since the token was issued
    return user


# --- wrong-password limit ----------------------------------------------------------

_failures = {}   # username -> (count, time of the last failure)
_failures_lock = threading.Lock()


def check_not_locked(username: str) -> None:
    with _failures_lock:
        count, last = _failures.get(username, (0, 0))
        if count >= MAX_FAILED_LOGINS and time.time() - last < LOCKOUT_SECONDS:
            raise HTTPException(status_code=429, detail="Too many wrong passwords. Wait a minute and try again.")


def record_login(username: str, ok: bool) -> None:
    with _failures_lock:
        if ok:
            _failures.pop(username, None)
            return
        count, last = _failures.get(username, (0, 0))
        if time.time() - last >= LOCKOUT_SECONDS:
            count = 0
        if len(_failures) > 10000:
            _failures.clear()
        _failures[username] = (count + 1, time.time())


def user_for_password(username: str, password: str) -> Optional[dict]:
    check_not_locked(username)
    user = load_user(username)
    ok = user is not None and password_matches(user, password)
    record_login(username, ok)
    return user if ok else None


# --- who is calling ----------------------------------------------------------------

def authenticate(authorization: Optional[str]) -> dict:
    """The user behind an Authorization header (Basic or Bearer), or 401."""
    scheme, _, value = (authorization or "").partition(" ")
    user = None
    if scheme.lower() == "bearer" and value:
        user = user_for_token(value.strip())
    elif scheme.lower() == "basic" and value:
        try:
            username, _, password = base64.b64decode(value.strip()).decode("utf-8").partition(":")
        except (binascii.Error, UnicodeError):
            username, password = "", ""
        user = user_for_password(username, password) if username else None
    if user is None:
        raise HTTPException(status_code=401, detail="Wrong username or password, or the sign-in has expired")
    return user


def current_user(authorization: Optional[str] = Header(None)) -> dict:
    return authenticate(authorization)


def admin(user: dict = Depends(current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can do this")
    return user


# --- API ---------------------------------------------------------------------------

@app.post("/login")
def login(username: str = Body(...), password: str = Body(...)):
    """Sign in. Returns a token to send as `Authorization: Bearer <token>`."""
    user = user_for_password(username, password)
    if user is None:
        raise HTTPException(status_code=401, detail="Wrong username or password")
    token, expires_at = make_token(user)
    return {"token": token, "expires_at": expires_at, **public(user)}


@app.get("/verify")
def verify(user: dict = Depends(current_user)):
    """Used by the other services: who does this Authorization header belong to?"""
    return public(user)


@app.get("/me")
def me(user: dict = Depends(current_user)):
    return public(user)


@app.post("/change_password")
def change_password(
    current_password: str = Body(...),
    new_password: str = Body(...),
    user: dict = Depends(current_user),
):
    """Change your own password. Returns a new token; older tokens stop working."""
    if not password_matches(user, current_password):
        raise HTTPException(status_code=403, detail="The current password is wrong")
    if new_password == current_password:
        raise HTTPException(status_code=400, detail="The new password must differ from the current one")
    check_new_password(new_password)
    set_password(user["username"], new_password, must_change_password=False)
    updated = load_user(user["username"])
    token, expires_at = make_token(updated)
    return {"token": token, "expires_at": expires_at, **public(updated)}


@app.get("/users")
def list_users(_: dict = Depends(admin)):
    return {"users": [public(u) for u in map(load_user, sorted(zk.get_children(USERS_PATH))) if u]}


@app.post("/users")
def create_user(
    username: str = Body(...),
    password: str = Body(...),
    role: str = Body("user"),
    _: dict = Depends(admin),
):
    if role not in ROLES:
        raise HTTPException(status_code=400, detail=f"Role must be one of: {', '.join(ROLES)}")
    check_new_password(password)
    try:
        save_new_user(username, password, role, must_change_password=False)
    except NodeExistsError:
        raise HTTPException(status_code=409, detail="That username is taken")
    return public(load_user(username))


@app.put("/users/{username}/password")
def reset_password(username: str, new_password: str = Body(..., embed=True), _: dict = Depends(admin)):
    """An administrator sets someone's password; they are asked to change it at the next sign-in."""
    check_new_password(new_password)
    set_password(username, new_password, must_change_password=True)
    return public(load_user(username))


@app.delete("/users/{username}")
def delete_user(username: str, _: dict = Depends(admin)):
    target = load_user(username)
    if target is None:
        raise HTTPException(status_code=404, detail="User not found")
    admins = [u for u in map(load_user, zk.get_children(USERS_PATH)) if u and u["role"] == "admin"]
    if target["role"] == "admin" and len(admins) <= 1:
        raise HTTPException(status_code=400, detail="The last administrator cannot be deleted")
    zk.delete(user_path(username))
    return {"status": f"User {username} deleted"}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
