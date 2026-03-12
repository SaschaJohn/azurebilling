from flask import Blueprint, g, jsonify, render_template, request

from app.services import chat_service

bp = Blueprint('chat', __name__, url_prefix='/chat')


@bp.get('/')
def index():
    return render_template('chat/index.html')


@bp.post('/ask')
def ask():
    data = request.get_json(force=True, silent=True) or {}
    question = (data.get('question') or '').strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    active_month = g.active_month  # may be None (all months)
    result = chat_service.ask(question, active_month)
    return jsonify(result)
