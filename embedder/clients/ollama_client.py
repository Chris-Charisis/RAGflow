from typing import Any, Dict, Optional
import requests


class OllamaClientError(RuntimeError):
    pass


class OllamaClient:
    """
    Thin HTTP client: owns base_url, timeouts, and a requests.Session.
    """
    def __init__(self, base_url: str, timeout_s: int = 60, session: Optional[requests.Session] = None):
        self.base_url = base_url
        self.timeout_s = timeout_s
        self.session = session or requests.Session()    

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def request(self, method: str, path: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            resp = self.session.request(
                method=method.upper(),
                url=self._url(path),
                json=json,
                timeout=self.timeout_s,
            )
        except requests.RequestException as e:
            raise OllamaClientError(f"HTTP error to Ollama: {e}") from e

        if resp.status_code != 200:
            raise OllamaClientError(f"Ollama {method.upper()} {path} -> {resp.status_code}: {resp.text}")

        try:
            return resp.json()
        except ValueError as e:
            raise OllamaClientError(f"Non-JSON response from Ollama: {e}") from e

    def health(self) -> bool:
        # A light ping against /api/tags is enough to confirm the daemon is up.
        try:
            _ = self.request("GET", "/api/tags", json=None)
            return True
        except OllamaClientError:
            return False
