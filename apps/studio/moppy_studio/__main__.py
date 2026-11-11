import argparse

import uvicorn

from .app import app


def main():
    parser = argparse.ArgumentParser(description="MOPPy Studio local preview")
    parser.add_argument("--port", type=int, default=8767)
    args = parser.parse_args()
    print(f"MOPPy Studio: http://127.0.0.1:{args.port}", flush=True)
    print("Session token: " + app.state.token, flush=True)
    uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
