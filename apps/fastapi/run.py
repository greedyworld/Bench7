import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8080")),
        workers=int(os.environ.get("WORKERS") or os.cpu_count() or 1),
        loop="uvloop",
        http="httptools",
        log_level="warning",
        access_log=False,
        backlog=65535,
        timeout_keep_alive=75,
    )
