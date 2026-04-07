from flask import Flask, render_template, request, redirect, url_for, session, flash
from dotenv import load_dotenv
from datetime import date, datetime
import os
import database
from pytz import timezone

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "fallback-secret")

database.init_db()

USERNAME = os.getenv("IDENTITY")
PASSWORD = os.getenv("PASSWORD")


def login_required(f):
    from functools import wraps

    @wraps(f)
    def decorated(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/", methods=["GET"])
@login_required
def index():
    india = timezone("Asia/Kolkata")
    selected_date = request.args.get(
        "date", datetime.now(india).date().isoformat())
    entries = database.get_entries_by_date(selected_date)
    total = database.get_total_calories(selected_date)
    all_dates = database.get_all_dates()
    today = datetime.now(india).date().isoformat()

    # Ensure today is always in the date list for the sidebar
    if today not in all_dates:
        all_dates = [today] + list(all_dates)

    return render_template(
        "dashboard.html",
        entries=entries,
        selected_date=selected_date,
        total=total,
        all_dates=all_dates,
        today=today
    )


@app.route("/add", methods=["POST"])
@login_required
def add():
    food_item = request.form.get("food_item", "").strip()
    calories = request.form.get("calories", "").strip()
    entry_date = request.form.get("entry_date", date.today().isoformat())

    if not food_item or not calories:
        flash("Both food item and calories are required.", "error")
        return redirect(url_for("index", date=entry_date))

    try:
        calories = int(calories)
        if calories <= 0:
            raise ValueError
    except ValueError:
        flash("Calories must be a positive number.", "error")
        return redirect(url_for("index", date=entry_date))

    database.add_entry(entry_date, food_item, calories)
    return redirect(url_for("index", date=entry_date))


@app.route("/delete/<int:entry_id>", methods=["POST"])
@login_required
def delete(entry_id):
    entry_date = request.form.get("entry_date", date.today().isoformat())
    database.delete_entry(entry_id)
    return redirect(url_for("index", date=entry_date))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if (request.form.get("username") == USERNAME and
                request.form.get("password") == PASSWORD):
            session["logged_in"] = True
            return redirect(url_for("index"))
        flash("Invalid credentials.", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(debug=True)
