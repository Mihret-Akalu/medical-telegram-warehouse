"""
Main FastAPI application
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

# Import routers
from api.routers import health, reports, search, channels

# Create FastAPI app
app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="Analytical API for Ethiopian Medical Telegram Channels",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(reports.router)
app.include_router(search.router)
app.include_router(channels.router)

@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to documentation"""
    return RedirectResponse(url="/docs")

@app.get("/status")
async def status():
    """API status"""
    return {
        "status": "online",
        "service": "Medical Telegram Warehouse API",
        "version": "1.0.0"
    }

# Add startup event to test database connection
@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    from api.database import test_connection
    result = test_connection()
    print(f"📊 Database connection: {result['status']}")
    if result['status'] == 'connected':
        print(f"   Tables found: {len(result['tables'])}")
    else:
        print(f"   Error: {result.get('message', 'Unknown error')}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)