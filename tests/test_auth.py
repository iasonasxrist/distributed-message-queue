"""Sign-in: the auth service, and the services that rely on it."""
import base64
import json

import pytest

NEW_PASSWORD = "a-much-better-password"


def basic(username="admin", password="admin"):
    return {"Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()}


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def login(cluster, username="admin", password="admin"):
    return cluster.auth.post("/login", json={"username": username, "password": password})


@pytest.fixture
def secured(cluster, monkeypatch):
    """Three brokers and the auth service, with sign-in switched on everywhere."""
    monkeypatch.setenv("AUTH_ENABLED", "1")
    cluster.start("b1", "b2", "b3")
    cluster.auth
    return cluster


def add_user(cluster, username="ada", password="correct-horse", role="user"):
    response = cluster.auth.post(
        "/users", json={"username": username, "password": password, "role": role}, headers=basic()
    )
    assert response.status_code == 200, response.text
    return basic(username, password)


# --- the auth service ---------------------------------------------------------------

def test_first_start_creates_the_default_administrator(secured):
    body = login(secured).json()
    assert (body["username"], body["role"], body["must_change_password"]) == ("admin", "admin", True)
    assert body["token"]


def test_default_credentials_can_be_set_in_the_settings(cluster, monkeypatch):
    monkeypatch.setenv("AUTH_DEFAULT_USER", "root")
    monkeypatch.setenv("AUTH_DEFAULT_PASSWORD", "starting-password")
    assert login(cluster, "root", "starting-password").status_code == 200
    assert login(cluster, "admin", "admin").status_code == 401


def test_restart_does_not_bring_the_default_password_back(secured, monkeypatch):
    secured.auth.post("/change_password", headers=basic(),
                      json={"current_password": "admin", "new_password": NEW_PASSWORD})
    secured._auth = None            # the auth service restarts
    assert login(secured, "admin", "admin").status_code == 401
    assert login(secured, "admin", NEW_PASSWORD).status_code == 200


@pytest.mark.parametrize("username, password", [("admin", "wrong"), ("nobody", "admin"), ("", ""), ("../x", "admin")])
def test_wrong_credentials_are_rejected(secured, username, password):
    assert login(secured, username, password).status_code == 401


def test_passwords_are_stored_hashed(secured):
    record = secured.zk.get("/auth/users/admin")[0].decode()
    assert "admin" == json.loads(record)["username"]
    assert '"admin"' not in record.replace('"username": "admin"', "").replace('"role": "admin"', "")
    assert len(json.loads(record)["hash"]) == 64


def test_verify_accepts_basic_and_token(secured):
    token = login(secured).json()["token"]
    assert secured.auth.get("/verify", headers=basic()).json()["username"] == "admin"
    assert secured.auth.get("/verify", headers=bearer(token)).json()["username"] == "admin"


@pytest.mark.parametrize("headers", [
    {}, {"Authorization": "Bearer not.a-token"}, {"Authorization": "Bearer "}, {"Authorization": "Basic !!!"},
    {"Authorization": "Digest abc"},
])
def test_verify_rejects_missing_or_broken_credentials(secured, headers):
    assert secured.auth.get("/verify", headers=headers).status_code == 401


def test_tampered_token_is_rejected(secured):
    payload, signature = login(secured).json()["token"].split(".")
    forged = base64.urlsafe_b64encode(json.dumps({"sub": "admin", "pv": 1, "exp": 9e12}).encode()).decode().rstrip("=")
    assert secured.auth.get("/verify", headers=bearer(f"{forged}.{signature}")).status_code == 401


def test_token_expires(secured, monkeypatch):
    monkeypatch.setattr(secured.auth_module, "TOKEN_HOURS", -1)
    token = login(secured).json()["token"]
    assert secured.auth.get("/verify", headers=bearer(token)).status_code == 401


def test_change_password(secured):
    old_token = login(secured).json()["token"]

    response = secured.auth.post("/change_password", headers=basic(),
                                 json={"current_password": "admin", "new_password": NEW_PASSWORD})

    assert response.status_code == 200
    assert response.json()["must_change_password"] is False
    assert login(secured, "admin", "admin").status_code == 401                       # old password is gone
    assert login(secured, "admin", NEW_PASSWORD).json()["must_change_password"] is False
    assert secured.auth.get("/verify", headers=bearer(old_token)).status_code == 401  # and so are old tokens
    assert secured.auth.get("/verify", headers=bearer(response.json()["token"])).status_code == 200


@pytest.mark.parametrize("current, new, status", [
    ("wrong", NEW_PASSWORD, 403),
    ("admin", "short", 400),
    ("admin", "admin", 400),
])
def test_change_password_rules(secured, current, new, status):
    response = secured.auth.post("/change_password", headers=basic(),
                                 json={"current_password": current, "new_password": new})
    assert response.status_code == status
    assert login(secured).status_code == 200


def test_repeated_wrong_passwords_lock_the_user_for_a_while(secured, monkeypatch):
    for _ in range(5):
        assert login(secured, "admin", "guess").status_code == 401

    assert login(secured, "admin", "admin").status_code == 429

    monkeypatch.setattr(secured.auth_module, "LOCKOUT_SECONDS", 0)   # the wait is over
    assert login(secured, "admin", "admin").status_code == 200


def test_administrator_manages_users(secured):
    ada = add_user(secured)

    listed = secured.auth.get("/users", headers=basic()).json()["users"]
    assert [(u["username"], u["role"]) for u in listed] == [("ada", "user"), ("admin", "admin")]
    assert secured.auth.get("/me", headers=ada).json()["role"] == "user"

    assert secured.auth.post("/users", headers=basic(),
                             json={"username": "ada", "password": "another-password"}).status_code == 409
    assert secured.auth.post("/users", headers=basic(),
                             json={"username": "bob", "password": "another-password", "role": "god"}).status_code == 400
    assert secured.auth.post("/users", headers=basic(),
                             json={"username": "bob", "password": "short"}).status_code == 400

    assert secured.auth.delete("/users/ada", headers=basic()).status_code == 200
    assert secured.auth.get("/me", headers=ada).status_code == 401


def test_only_administrators_manage_users(secured):
    ada = add_user(secured)
    assert secured.auth.get("/users", headers=ada).status_code == 403
    assert secured.auth.post("/users", headers=ada, json={"username": "x", "password": "long-enough-1"}).status_code == 403
    assert secured.auth.delete("/users/admin", headers=ada).status_code == 403


def test_the_last_administrator_cannot_be_deleted(secured):
    assert secured.auth.delete("/users/admin", headers=basic()).status_code == 400
    add_user(secured, "second", "second-admin-pw", role="admin")
    assert secured.auth.delete("/users/admin", headers=basic("second", "second-admin-pw")).status_code == 200


def test_administrator_resets_a_password(secured):
    add_user(secured)
    response = secured.auth.put("/users/ada/password", headers=basic(), json={"new_password": "temporary-pw"})
    assert response.json()["must_change_password"] is True
    assert login(secured, "ada", "correct-horse").status_code == 401
    assert login(secured, "ada", "temporary-pw").status_code == 200


# --- producer and consumer --------------------------------------------------------------

def test_producer_refuses_requests_without_sign_in(secured):
    response = secured.producer.post("/create_topic/news/1")
    assert response.status_code == 401
    assert response.headers["www-authenticate"].startswith("Basic")
    assert secured.producer.post("/write/news", json={"key": "k"}).status_code == 401
    assert secured.producer.delete("/delete_topic/news").status_code == 401
    assert not secured.zk.exists("/topics/news")


def test_producer_and_consumer_work_with_basic_credentials(secured):
    admin = basic()
    assert secured.producer.post("/create_topic/news/1", headers=admin).status_code == 200
    assert secured.producer.post("/write/news", json={"key": "k", "value": 1}, headers=admin).status_code == 200
    assert secured.consumer.post("/subscribe/a@b.c/news", headers=admin).status_code == 200
    body = secured.consumer.get("/read/a@b.c/news/k", headers=admin).json()
    assert [m["value"] for m in body["messages"]] == [1]


def test_producer_works_with_a_token(secured):
    token = bearer(login(secured).json()["token"])
    assert secured.producer.post("/create_topic/news/1", headers=token).status_code == 200
    assert secured.producer.post("/write/news", json={"key": "k"}, headers=token).status_code == 200


def test_consumer_refuses_requests_without_sign_in(secured):
    assert secured.consumer.post("/subscribe/a@b.c/news").status_code == 401
    assert secured.consumer.get("/read/a@b.c/news/k").status_code == 401
    assert secured.consumer.post("/unsubscribe/a@b.c/news").status_code == 401


def test_wrong_password_is_refused_by_every_service(secured):
    wrong = basic("admin", "nope")
    assert secured.producer.post("/write/news", json={"key": "k"}, headers=wrong).status_code == 401
    assert secured.consumer.get("/read/a@b.c/news/k", headers=wrong).status_code == 401
    assert secured.console.get("/api/cluster", headers=wrong).status_code == 401


def test_ordinary_users_publish_and_read_but_do_not_manage_topics(secured):
    ada = add_user(secured)
    secured.producer.post("/create_topic/news/1", headers=basic())

    assert secured.producer.post("/write/news", json={"key": "k", "value": 1}, headers=ada).status_code == 200
    assert secured.consumer.post("/subscribe/ada@x.y/news", headers=ada).status_code == 200
    assert secured.consumer.get("/read/ada@x.y/news/k", headers=ada).status_code == 200
    assert secured.producer.post("/create_topic/other/1", headers=ada).status_code == 403
    assert secured.producer.delete("/delete_topic/news", headers=ada).status_code == 403


def test_sign_in_is_remembered_between_requests(secured):
    secured.producer.post("/create_topic/news/1", headers=basic())
    secured.disconnect("auth")       # the auth service goes away

    # the same credentials keep working for a while ...
    assert secured.producer.post("/write/news", json={"key": "k"}, headers=basic()).status_code == 200
    # ... but nobody new can be checked
    assert secured.producer.post("/write/news", json={"key": "k"}, headers=basic("ada", "pw")).status_code == 503


def test_remembered_sign_in_expires(secured, monkeypatch):
    secured.producer.post("/create_topic/news/1", headers=basic())
    monkeypatch.setattr(secured.producer_module.time, "monotonic", lambda: 10 ** 9)   # much later
    secured.disconnect("auth")
    assert secured.producer.post("/write/news", json={"key": "k"}, headers=basic()).status_code == 503


# --- console -------------------------------------------------------------------------

def test_console_page_is_public_but_its_data_is_not(secured):
    assert secured.console.get("/").status_code == 200
    assert secured.console.get("/healthz").status_code == 200
    response = secured.console.get("/api/cluster")
    assert response.status_code == 401
    assert "www-authenticate" not in response.headers      # the page shows its own sign-in form
    assert secured.console.get("/api/messages/news/0").status_code == 401
    assert secured.console.get("/api/settings").status_code == 401


def test_console_sign_in_and_snapshot(secured):
    response = secured.console.post("/api/login", json={"username": "admin", "password": "admin"})
    assert response.status_code == 200
    token = bearer(response.json()["token"])

    snapshot = secured.console.get("/api/cluster", headers=token).json()

    assert snapshot["user"] == {"username": "admin", "role": "admin", "must_change_password": True}
    assert snapshot["settings"]["read_only"] is False


def test_console_sign_in_with_a_wrong_password(secured):
    response = secured.console.post("/api/login", json={"username": "admin", "password": "nope"})
    assert response.status_code == 401


def test_console_change_password(secured):
    token = bearer(secured.console.post("/api/login", json={"username": "admin", "password": "admin"}).json()["token"])
    secured.console.get("/api/cluster", headers=token)

    response = secured.console.post("/api/change_password", headers=token,
                                    json={"current_password": "admin", "new_password": NEW_PASSWORD})

    assert response.status_code == 200
    assert secured.console.get("/api/cluster", headers=token).status_code == 401           # old sign-in ended at once
    new_token = bearer(response.json()["token"])
    assert secured.console.get("/api/cluster", headers=new_token).json()["user"]["must_change_password"] is False


def test_only_administrators_change_retention_or_purge(secured):
    ada = add_user(secured)
    secured.producer.post("/create_topic/news/1", headers=basic())

    assert secured.console.get("/api/cluster", headers=ada).json()["settings"]["read_only"] is True
    assert secured.console.post("/api/purge/news", json={"older_than_hours": 0}, headers=ada).status_code == 403
    assert secured.console.put("/api/settings/retention", json={"default_hours": 1}, headers=ada).status_code == 403
    assert secured.console.put("/api/settings/retention/news", json={"hours": 1}, headers=ada).status_code == 403
    assert secured.console.post("/api/purge/news", json={"older_than_hours": 0}, headers=basic()).status_code == 200


def test_sign_in_can_be_switched_off(three_brokers):
    """AUTH_ENABLED=0: everything is open, as before."""
    assert three_brokers.producer.post("/create_topic/news/1").status_code == 200
    assert three_brokers.console.get("/api/cluster").json()["user"] is None


# --- the brokers' internal API ---------------------------------------------------------

@pytest.fixture
def sealed(cluster, monkeypatch):
    """Sign-in on, and brokers only answer the cluster's own services."""
    monkeypatch.setenv("AUTH_ENABLED", "1")
    monkeypatch.setenv("INTERNAL_TOKEN", "cluster-secret")
    cluster.start("b1", "b2", "b3")
    cluster.auth
    return cluster


def test_brokers_refuse_direct_calls_without_the_internal_token(sealed):
    sealed.producer.post("/create_topic/news/1", headers=basic())
    broker = sealed.broker("b1")

    assert broker.post("/write/news/0", json={"key": "k"}).status_code == 401
    assert broker.get("/read/news/0").status_code == 401
    assert broker.post("/create_topic/sneaky/1").status_code == 401
    assert broker.post("/write/news/0", json={"key": "k"}, headers={"X-Internal-Token": "guess"}).status_code == 401
    assert sealed.log("b1", "news") is None


def test_the_cluster_works_normally_with_the_internal_token(sealed):
    admin = basic()
    sealed.producer.post("/create_topic/news/1", headers=admin)
    assert sealed.producer.post("/write/news", json={"key": "k", "value": 1}, headers=admin).status_code == 200
    assert sealed.log("b2", "news") == [{"key": "k", "value": 1}]          # replication between brokers
    sealed.consumer.post("/subscribe/a@b.c/news", headers=admin)
    assert sealed.consumer.get("/read/a@b.c/news/k", headers=admin).status_code == 200
    partition = sealed.console.get("/api/cluster", headers=admin).json()["topics"][0]["partitions"][0]
    assert [r["end_offset"] for r in partition["replicas"]] == [1, 1, 1]

    sealed.kill("b1")
    sealed.start_broker("b1")                                              # rejoin copies from the leader
    assert sealed.log("b1", "news") == [{"key": "k", "value": 1}]
