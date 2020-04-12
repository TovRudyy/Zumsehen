import logging
import os
from typing import List, Optional

from flask import flash, redirect, render_template, request, url_for

from src.Trace import Trace
from src.interface import app
from src.persistence.controller import parse_trace

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

traces_path: Optional[str] = None
traces: List[Trace] = []
current_trace: Optional[Trace] = None


ALLOWED_EXTENSIONS = {"prv", "hdf"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/")
@app.route("/index")
def index():
    trace_options = []
    if traces_path is not None:
        files = [f for f in os.listdir(traces_path) if os.path.isfile(os.path.join(traces_path, f))]
        trace_options = [f for f in files if allowed_file(f)]
    return render_template("index.html", trace_options=trace_options)


@app.route("/select_path", methods=["GET", "POST"])
def select_path():
    global traces_path
    path = request.form.get("traces_path")
    if not os.path.exists(path):
        flash("Path doesn't exist.")
    elif not os.path.isdir(path):
        flash("Path is not a directory.")
    else:
        traces_path = path
    return redirect(url_for("index"))


@app.route("/upload_trace", methods=["GET", "POST"])
def upload_trace():
    global traces, current_trace
    selected_trace = request.form.get("selected_trace")
    if selected_trace == "":
        flash("No trace selected.")
        logger.info("No trace selected.")
        return redirect(url_for("index"))

    trace_file = os.path.join(traces_path, selected_trace)
    logger.info(selected_trace)
    if allowed_file(selected_trace):
        trace = parse_trace(trace_file)

        current_trace = trace
        traces.append(trace)

        flash(f"Trace {selected_trace} uploaded.")
        logger.info(f"Trace {selected_trace} uploaded.")
    else:
        logger.info(f"Invalid file format. Allowed formats are: {', '.join(ALLOWED_EXTENSIONS)}")
        flash(f"Invalid file format. Allowed formats are: {', '.join(ALLOWED_EXTENSIONS)}")

    return redirect(url_for("index"))


def get_current_trace_text(trace):
    if trace is not None:
        return trace
    else:
        return "no trace selected"


@app.route("/analyze")
def analyze():
    global traces, current_trace
    logger.info(traces)
    traces_names = [trace.metadata.name for trace in traces]
    current_trace_info = str(get_current_trace_text(current_trace))
    return render_template("analyze.html", traces=traces_names, current_trace_info=current_trace_info)


@app.route("/select_trace")
def select_trace():
    global traces, current_trace
    logger.info(request.args)
    selected_trace = request.args["selected_trace"]
    for trace in traces:
        if trace.metadata.name == selected_trace:
            current_trace = trace
            break
    return redirect(url_for("analyze"))
