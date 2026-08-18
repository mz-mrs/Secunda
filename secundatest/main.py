from fastapi import FastAPI
from sqlalchemy import text

from secundatest.db.session import async_session_factory
from secundatest.api.payments import router as payments_router

app = FastAPI(title="Payment Processing Service for Secunda (TestTask)")

app.include_router(payments_router)

@app.get("/health")
async def health():
    async with async_session_factory() as session:
        await session.execute(text("SELECT 1"))
    return {"status": "bingo!"}