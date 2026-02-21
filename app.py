"""
Flask web application for Financial Statement Extraction.
Wraps the CLI extract_financials.py tool with a web upload/download interface.
"""

import io
import os
import shutil
import tempfile
import logging

from flask import Flask, request, send_file, render_template, jsonify
from werkzeug.utils import secure_filename

from extract_financials import FinancialExtractor

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("WebApp")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("MAX_UPLOAD_MB", 100)) * 1024 * 1024


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    filename = secure_filename(file.filename)
    if not filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files are accepted"}), 400

    tmp_dir = tempfile.mkdtemp()
    try:
        pdf_path = os.path.join(tmp_dir, filename)
        file.save(pdf_path)

        output_name = filename.rsplit(".", 1)[0] + "_extracted.xlsx"
        output_path = os.path.join(tmp_dir, output_name)

        log.info(f"Processing: {filename}")
        extractor = FinancialExtractor(pdf_path, output_path)
        result_path = extractor.run()

        if not os.path.exists(result_path):
            return jsonify({"error": "Extraction produced no output"}), 500

        # Read into memory so we can clean up temp files immediately
        buf = io.BytesIO()
        with open(result_path, "rb") as f:
            buf.write(f.read())
        buf.seek(0)

        return send_file(
            buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=output_name,
        )
    except Exception as e:
        log.exception("Extraction failed")
        return jsonify({"error": f"Extraction failed: {str(e)}"}), 500
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@app.errorhandler(413)
def file_too_large(e):
    max_mb = app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024)
    return jsonify({"error": f"File too large. Maximum size is {max_mb} MB."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
