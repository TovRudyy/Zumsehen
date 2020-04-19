import logging
import os
from typing import Optional, Dict

from flask import flash, redirect, render_template, request, url_for

from src.Trace import Trace
from src.interface import app
from src.persistence.controller import parse_trace

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

traces_path: Optional[str] = None
traces: Dict[str, Trace] = dict()
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
    if traces_path is not None:
        path_text = traces_path
    else:
        path_text = "No path selected yet."
    return render_template("index.html", trace_options=trace_options, path_text=path_text)


@app.route("/select_path", methods=["GET", "POST"])
def select_path():
    global traces_path
    path = request.form.get("traces_path")
    logger.debug(path)
    if path == "" or path is None:
        flash("Please introduce a path.")
    elif not os.path.exists(path):
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
        traces[trace.metadata.name] = trace

        flash(f"Trace {selected_trace} uploaded.")
        logger.info(f"Trace {selected_trace} uploaded.")
    else:
        logger.info(f"Invalid file format. Allowed formats are: {', '.join(ALLOWED_EXTENSIONS)}")
        flash(f"Invalid file format. Allowed formats are: {', '.join(ALLOWED_EXTENSIONS)}")

    return redirect(url_for("index"))


@app.route("/analyze")
def analyze():
    logger.info(traces)
    return render_template("analyze.html", traces=traces, current_trace=current_trace)


@app.route("/select_trace")
def select_trace():
    global current_trace
    logger.info(request.args)
    selected_trace_name = request.args["selected_trace_name"]
    current_trace = traces[selected_trace_name]
    return redirect(url_for("analyze"))


@app.route("/drop_trace")
def drop_trace():
    global traces, current_trace
    logger.info(request.args)
    droped_trace_name = request.args["droped_trace_name"]
    if droped_trace_name == current_trace.metadata.name:
        current_trace = None
    del traces[droped_trace_name]
    return redirect(url_for("analyze"))
