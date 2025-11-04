import os
import requests
import json
import sys

def pull_model(model_name, server_url):
    """
    Pull a model from an Ollama server using the /api/pull endpoint.
    """
    url = f"{server_url.rstrip('/')}/api/pull"
    payload = {"name": model_name}

    print(f"🔄 Pulling model '{model_name}' from {server_url}...")

    try:
        with requests.post(url, json=payload, stream=True) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if not line:
                    continue
                try:
                    data = json.loads(line.decode("utf-8"))
                    if "status" in data:
                        print("➡️", data["status"])
                    elif "error" in data:
                        print("❌ Error:", data["error"])
                except json.JSONDecodeError:
                    print("⚠️ Non-JSON response:", line.decode("utf-8"))
        print("✅ Model pull complete.")
    except requests.RequestException as e:
        print("❌ Request failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    server_url = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    model_name = os.getenv("MODEL")

    if not model_name:
        print("❌ Environment variable MODEL is not set.")
        print("Example:")
        print("  export MODEL=mistral")
        print("  export OLLAMA_HOST=http://localhost:11434")
        sys.exit(1)

    pull_model(model_name, server_url)
