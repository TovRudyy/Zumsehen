import logging
from typing import List, Optional

from flask import flash, g, redirect, render_template, request, url_for

from src.interface import app
from src.persistence.controller import parse_trace
from src.Trace import Trace

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

traces: List[Trace] = []
current_trace: Optional[Trace] = None


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")


ALLOWED_EXTENSIONS = {"prv", "hdf"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/upload_trace", methods=["GET", "POST"])
def upload_trace():
    global traces, current_trace
    if "trace_file" not in request.files or request.files["trace_file"].filename == "":
        flash("No trace selected.")
        logger.info("No trace selected.")
        return redirect(url_for("index"))

    trace_file = request.files["trace_file"]
    filename = str(trace_file.filename)
    logger.info(filename)
    logger.info(type(filename))
    if allowed_file(filename):
        trace = parse_trace(trace_file)

        current_trace = trace
        traces.append(trace)

        flash(f"Trace {filename} uploaded.")
        logger.info(f"Trace {filename} uploaded.")
    else:
        logger.info(f"Invalid file format. Allowed formats are: {', '.join(ALLOWED_EXTENSIONS)}")
        flash(f"Invalid file format. Allowed formats are: {', '.join(ALLOWED_EXTENSIONS)}")

    return redirect(url_for("index"))


def get_current_trace_text(trace):
    if trace is not None:
        return trace
    else:
        return "no trace selected"


def render_analyze():
    global traces, current_trace
    traces_names = [trace.metadata.name for trace in traces]
    current_trace_info = get_current_trace_text(current_trace)
    return render_template("analyze.html", traces=traces_names, current_trace_info=current_trace_info)


@app.route("/analyze")
def analyze():
    global traces, current_trace
    logger.info(traces)
    return render_analyze()


@app.route("/select_trace")
def select_trace():
    global traces, current_trace
    logger.info(request.args)
    selected_trace = request.args["selected_trace"]
    for trace in traces:
        if trace.metadata.name == selected_trace:
            current_trace = trace
            break
    return render_analyze()
