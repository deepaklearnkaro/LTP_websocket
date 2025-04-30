import os
import asyncio
import httpx
import datetime
from dotenv import load_dotenv
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Dict, Set
import json

load_dotenv()
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

router = APIRouter()

# Store active WebSocket connections
active_connections: Dict[int, Set[WebSocket]] = {}

class LTPRequest(BaseModel):
    security_id: int

async def get_ltp(security_id: int, max_retries: int = 3) -> float:
    """Fetch LTP from DHAN API with retry logic"""
    url = "https://api.dhan.co/v2/marketfeed/ltp"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
    }

    payload = {exchange: [security_id] for exchange in ["BSE_FNO", "BSE_EQ", "NSE_FNO", "NSE_EQ", "MCX_COMM", "IDX_I"]}

    for attempt in range(2, max_retries + 1):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    security_id_str = str(security_id)
                    for exchange, securities in data.get("data", {}).items():
                        if security_id_str in securities:
                            ltp = securities[security_id_str].get("last_price")
                            if ltp is not None:
                                print(f"✅ [LTP Found] Security ID: {security_id} | Price: {ltp} | Exchange: {exchange}")
                                return float(ltp)
                    
                    print(f"❌ [LTP Not Found] Security ID: {security_id} | Attempt {attempt}/{max_retries}")
                else:
                    print(f"❌ [API Error] Security ID: {security_id} | Status: {response.status_code} | Attempt {attempt}/{max_retries}")
                
        except Exception as e:
            print(f"❌ [Exception] Security ID: {security_id} | Error: {str(e)} | Attempt {attempt}/{max_retries}")
        
        if attempt < max_retries:
            await asyncio.sleep(2)

    raise ValueError(f"🔴 [Final Failure] Could not fetch LTP for security ID {security_id}")
@router.websocket("/ws/ltp/{security_id}")
async def websocket_ltp(websocket: WebSocket, security_id: int):
    """WebSocket endpoint for real-time LTP updates"""
    await websocket.accept()

    # Register connection
    if security_id not in active_connections:
        active_connections[security_id] = set()
    active_connections[security_id].add(websocket)

    print(f"✅ [Connection Accepted] Security ID: {security_id} | Total connections: {len(active_connections[security_id])}")

    try:
        last_ltp = None
        while True:
            try:
                # Non-blocking receive from frontend
                try:
                    message = await asyncio.wait_for(websocket.receive_text(), timeout=0.1)
                    data = json.loads(message)
                    if data.get("action") == "stop":
                        print(f"🛑 [Stop Action Received] Security ID: {security_id}")
                        break
                except asyncio.TimeoutError:
                    pass  # No incoming message, continue polling

                # Fetch and send LTP if changed
                current_ltp = await get_ltp(security_id)
                if current_ltp != last_ltp:
                    update_msg = {
                        "security_id": security_id,
                        "ltp": current_ltp,
                        "timestamp": datetime.datetime.now().isoformat(),
                        "status": "success"
                    }
                    await websocket.send_json(update_msg)
                    last_ltp = current_ltp
                    print(f"🔄 [Update Sent] Security ID: {security_id} | Price: {current_ltp}")

                await asyncio.sleep(1)

            except ValueError as e:
                print(f"⚠️ [LTP Fetch Error] Security ID: {security_id} | Error: {str(e)}")
                await asyncio.sleep(5)

    except WebSocketDisconnect:
        print(f"🚪 [WebSocket Disconnect] Security ID: {security_id}")
    finally:
        # Clean up connections
        if security_id in active_connections and websocket in active_connections[security_id]:
            active_connections[security_id].remove(websocket)
            if not active_connections[security_id]:
                del active_connections[security_id]
        print(f"🧹 [Cleaned Up] Security ID: {security_id} | Remaining: {len(active_connections.get(security_id, set()))}")
# end 