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

    history = data.get('history') or []
    # Validate history is a list of {role, content} dicts; drop anything malformed
    history = [
        {"role": m["role"], "content": str(m["content"])}
        for m in history
        if isinstance(m, dict) and m.get("role") in ("user", "assistant") and m.get("content")
    ]
    active_month = g.active_month  # may be None (all months)
    result = chat_service.ask(question, active_month, history)
    return jsonify(result)
