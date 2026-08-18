from fastapi import FastAPI


app = FastAPI(title="Payment Processing Service for Secunda (TestTask)")


@app.get("/health")
async def health():
    return {"status": "bingo!"}