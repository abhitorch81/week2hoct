import os, uvicorn
if __name__ == "__main__":
    uvicorn.run("overturn.server:app", host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
