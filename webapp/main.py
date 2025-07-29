from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from webapp import models, api
from webapp.database import engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="SFTP File Import Manager",
    description="API для управления серверами и отслеживания статусов импорта файлов.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api.router, prefix="/api", tags=["SFTP Manager"])


@app.get("/")
def read_root():
    return {"message": "Welcome to SFTP File Import Manager. Visit /docs for API documentation."}
