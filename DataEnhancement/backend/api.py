from flask import Flask, request, jsonify
from dotenv import load_dotenv
import os
import pandas as pd
import uuid
import logging

# LinkedIn only
from scraper.linkedinScraper.main import run_batches

app = Flask(__name__)
load_dotenv()

@app.route("/api/linkedin-info-batch", methods=["POST"])
def get_linkedin_info_batch():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return jsonify({"error": "Expected a list of objects"}), 400

        # Normalize column names
        df = pd.DataFrame(data_list)
        df.rename(columns=lambda col: col.capitalize(), inplace=True)

        if df.empty or "Company" not in df.columns:
            return jsonify({"error": "Missing or empty 'Company' column"}), 400

        client_id = f"api_{uuid.uuid4().hex[:8]}"
        results = run_batches(df, client_id=client_id)

        return jsonify(results), 200

    except Exception as e:
        logging.error(f"🔥 API Fatal error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
