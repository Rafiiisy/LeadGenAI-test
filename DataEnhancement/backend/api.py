from flask import Flask, request, jsonify
from dotenv import load_dotenv
import os
import pandas as pd
import uuid
import logging

from scraper.linkedinScraper.main import run_batch

app = Flask(__name__)
load_dotenv()

@app.route("/api/linkedin-info-batch", methods=["POST"])
def get_linkedin_info_batch():
    try:
        payload = request.get_json()

        if not isinstance(payload, dict):
            return jsonify({"error": "Expected a JSON object with 'data' and 'li_at'"}), 400

        data_list = payload.get("data")
        li_at = payload.get("li_at")

        if not isinstance(data_list, list):
            return jsonify({"error": "Field 'data' must be a list"}), 400
        if not isinstance(li_at, str) or not li_at.strip():
            return jsonify({"error": "Field 'li_at' must be a non-empty string"}), 400

        # Convert and normalize DataFrame
        df = pd.DataFrame(data_list)
        df.rename(columns=lambda col: col.capitalize(), inplace=True)

        if df.empty or "Company" not in df.columns:
            return jsonify({"error": "Missing or empty 'Company' column"}), 400

        client_id = f"api_{uuid.uuid4().hex[:8]}"
        results = run_batch(df, client_id=client_id, li_at=li_at)

        return jsonify(results), 200

    except Exception as e:
        logging.error(f"🔥 API Fatal error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
