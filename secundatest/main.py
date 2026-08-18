from fastapi import FastAPI
from sqlalchemy import text

from secundatest.db.session import async_session_factory

app = FastAPI(title="Payment Processing Service for Secunda (TestTask)")


@app.get("/health")
async def health():
    async with async_session_factory() as session:
        await session.execute(text("SELECT 1"))
    return {"status": "bingo!"}