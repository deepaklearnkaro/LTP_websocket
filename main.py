# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from instrument_routes import router as instrument_router, load_csv_to_duckdb
from ltp_routes import router as ltp_router
from fastapi import WebSocket, WebSocketDisconnect


app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(instrument_router)
app.include_router(ltp_router)

# Load CSV at startup
@app.on_event("startup")
def startup_event():
    load_csv_to_duckdb()


@app.get("/",tags=["Root"])
def read_root():
   return {"Welcome to FastApi"}