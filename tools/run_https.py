"""Run the API with a local self-signed HTTPS certificate."""

import os
from pathlib import Path

from climatedata_api.app import app


ROOT = Path(__file__).resolve().parents[1]
CERTIFICATE = Path(os.getenv("CLIMATEDATA_TLS_CERT", ROOT / ".certs" / "localhost-cert.pem"))
KEY = Path(os.getenv("CLIMATEDATA_TLS_KEY", ROOT / ".certs" / "localhost-key.pem"))


if __name__ == "__main__":
    if not CERTIFICATE.is_file() or not KEY.is_file():
        raise SystemExit("TLS files are missing; run `mise run cert` first")

    app.run(
        host=os.getenv("CLIMATEDATA_HOST", "127.0.0.1"),
        port=int(os.getenv("CLIMATEDATA_PORT", "5443")),
        ssl_context=(str(CERTIFICATE), str(KEY)),
        use_reloader=False,
    )
