from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import asyncpg
from typing import Optional
import os
import uvicorn


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)
    await init_db(app.state.pool)
    yield
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/reviews_db")


async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                text TEXT NOT NULL,
                sentiment TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        ''')


class ReviewRequest(BaseModel):
    text: str


class ReviewResponse(BaseModel):
    id: int
    text: str
    sentiment: str
    created_at: str


def analyze_sentiment(text: str) -> str:
    text_lower = text.lower()
    if any(word in text_lower for word in ["хорош", "люблю"]):
        return "positive"
    elif any(word in text_lower for word in ["плохо", "ненавиж"]):
        return "negative"
    return "neutral"


@app.post("/reviews", response_model=ReviewResponse)
async def create_review(review: ReviewRequest):
    sentiment = analyze_sentiment(review.text)
    created_at = datetime.now()

    async with app.state.pool.acquire() as conn:
        review_id = await conn.fetchval(
            "INSERT INTO reviews (text, sentiment, created_at) VALUES ($1, $2, $3) RETURNING id",
            review.text, sentiment, created_at
        )

        new_review = await conn.fetchrow(
            "SELECT id, text, sentiment, created_at FROM reviews WHERE id = $1",
            review_id
        )
        return dict(new_review)


@app.get("/reviews", response_model=list[ReviewResponse])
async def get_reviews(sentiment: Optional[str] = None):
    async with app.state.pool.acquire() as conn:
        if sentiment:
            reviews = await conn.fetch(
                "SELECT id, text, sentiment, created_at FROM reviews WHERE sentiment = $1 ORDER BY created_at DESC",
                sentiment
            )
        else:
            reviews = await conn.fetch(
                "SELECT id, text, sentiment, created_at FROM reviews ORDER BY created_at DESC"
            )
        return [dict(review) for review in reviews]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)