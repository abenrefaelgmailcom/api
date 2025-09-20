from fastapi import FastAPI, HTTPException, Depends, Response, status, Query
import sqlite3
from typing import Generator, List, Dict, Any

DB_NAME = "messages.db"

app = FastAPI(title="REST API with SQLite – messages")

# ---------- DB utils ----------

def get_conn() -> Generator[sqlite3.Connection, None, None]:
    """
    Open a per-request SQLite connection and ensure rows behave like dicts.
    """
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # allow dict-like access: row["id"], row["text"]
    try:
        yield conn
    finally:
        conn.close()

def init_db() -> None:
    """
    Create table if not exists (runs once on startup).
    """
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
              id   INTEGER PRIMARY KEY AUTOINCREMENT,
              text TEXT NOT NULL
            );
        """)
        conn.commit()
    finally:
        conn.close()

@app.on_event("startup")
def _startup() -> None:
    init_db()

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {"id": row["id"], "text": row["text"]}

# ---------- Routes ----------

# GET /messages  -> SELECT * FROM messages;
@app.get("/messages")
def get_messages(conn: sqlite3.Connection = Depends(get_conn)) -> List[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM messages;")
    rows = cur.fetchall()
    return [row_to_dict(r) for r in rows]

# GET /messages/{id}  -> SELECT * FROM messages WHERE id = ?;
@app.get("/messages/{id}")
def get_message(id: int, conn: sqlite3.Connection = Depends(get_conn)) -> Dict[str, Any]:
    cur = conn.execute("SELECT * FROM messages WHERE id = ?;", (id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")
    return row_to_dict(row)

# POST /messages  -> INSERT INTO messages (text) VALUES (?);
# keeping same shape as your original code: text comes as a query parameter (?text=hello)
@app.post("/messages", status_code=status.HTTP_201_CREATED)
def create_message(
    text: str = Query(..., description="Message text"),
    conn: sqlite3.Connection = Depends(get_conn),
) -> Dict[str, Any]:
    cur = conn.execute("INSERT INTO messages (text) VALUES (?);", (text,))
    conn.commit()
    new_id = cur.lastrowid
    # return the inserted row
    cur = conn.execute("SELECT * FROM messages WHERE id = ?;", (new_id,))
    return row_to_dict(cur.fetchone())

# PUT /messages/{id}
# If row exists -> UPDATE; otherwise -> INSERT (id, text)
@app.put("/messages/{id}")
def put_message(
    id: int,
    text: str = Query(..., description="Message text"),
    response: Response = None,
    conn: sqlite3.Connection = Depends(get_conn),
) -> Dict[str, Any]:
    cur = conn.execute("UPDATE messages SET text = ? WHERE id = ?;", (text, id))
    if cur.rowcount == 0:
        # create if not exists
        conn.execute("INSERT INTO messages (id, text) VALUES (?, ?);", (id, text))
        conn.commit()
        if response is not None:
            response.status_code = status.HTTP_201_CREATED
    else:
        conn.commit()

    cur = conn.execute("SELECT * FROM messages WHERE id = ?;", (id,))
    return row_to_dict(cur.fetchone())

# PATCH /messages/{id}  -> UPDATE only if exists, else 404
@app.patch("/messages/{id}")
def patch_message(
    id: int,
    text: str = Query(..., description="New text"),
    conn: sqlite3.Connection = Depends(get_conn),
) -> Dict[str, Any]:
    cur = conn.execute("UPDATE messages SET text = ? WHERE id = ?;", (text, id))
    if cur.rowcount == 0:
        conn.rollback()
        raise HTTPException(status_code=404, detail="Message not found")
    conn.commit()
    cur = conn.execute("SELECT * FROM messages WHERE id = ?;", (id,))
    return row_to_dict(cur.fetchone())

# DELETE /messages/{id}  -> DELETE FROM messages WHERE id = ?;
@app.delete("/messages/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_message(id: int, conn: sqlite3.Connection = Depends(get_conn)) -> None:
    cur = conn.execute("DELETE FROM messages WHERE id = ?;", (id,))
    if cur.rowcount == 0:
        conn.rollback()
        raise HTTPException(status_code=404, detail="Message not found")
    conn.commit()
    # 204 No Content => nothing to return