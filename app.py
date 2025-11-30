# app.py
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from dash import Dash, html, dcc, Input, Output
import plotly.express as px

CSV_PATH = r"C:\Users\User\Desktop\JoyKingdom\DailyReport.xlsx"  # 👈 new file name

# ----------------------------
# Helpers: column detection
# ----------------------------
DATE_CANDIDATES = [
    "created_at", "payment_date", "date", "sale_date", "created", "order_date", "updated_at"
]
NAME_FIRST_CANDIDATES = ["sales_rep_firstname", "first_name", "firstname", "sales_rep_name", "sales_rep"]
NAME_LAST_CANDIDATES = ["sales_rep_lastname", "last_name", "lastname"]
NAME_SINGLE_CANDIDATES = ["Name", "name", "sales_rep", "sales_rep_fullname"]
STATUS_CANDIDATES = ["order_status", "payment_status", "status", "external_status"]
PRICE_CANDIDATES = ["product_price", "price", "sale_amount", "amount"]
PROVINCE_CANDIDATES = [
    "client_location.province", "province", "client_province", "client_location_province", "client_location.province"
]
SUBURB_CANDIDATES = ["client_location.suburb", "suburb", "client_location_suburb", "suburb_name"]
STREET_CANDIDATES = ["client_physical_address", "client_location.street_name", "street", "address", "client_location.street_name"]

def detect_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

# ----------------------------
# Load & preprocess (robust)
# ----------------------------
def load_and_prepare(csv_path=CSV_PATH):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Data file not found at {csv_path}")

    # ✅ Automatically detect Excel or CSV
    if csv_path.endswith(".xlsx") or csv_path.endswith(".xls"):
        df = pd.read_excel(csv_path)
    else:
        df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")

    # Detect key columns dynamically
    col_date = detect_column(df, DATE_CANDIDATES)
    col_first = detect_column(df, NAME_FIRST_CANDIDATES)
    col_last = detect_column(df, NAME_LAST_CANDIDATES)
    col_name_single = detect_column(df, NAME_SINGLE_CANDIDATES)
    col_status = detect_column(df, STATUS_CANDIDATES)
    col_price = detect_column(df, PRICE_CANDIDATES)
    col_province = detect_column(df, PROVINCE_CANDIDATES)
    col_suburb = detect_column(df, SUBURB_CANDIDATES)
    col_street = detect_column(df, STREET_CANDIDATES)

    print("Column detection results:")
    print(f" date: {col_date}")
    print(f" first: {col_first}, last: {col_last}, single-name: {col_name_single}")
    print(f" status: {col_status}")
    print(f" price: {col_price}")
    print(f" province: {col_province}")
    print(f" suburb: {col_suburb}")
    print(f" street: {col_street}")
    print("----")

    # Build Name
    if col_name_single:
        df["Name"] = df[col_name_single].astype(str)
    else:
        if col_first and col_last:
            df["Name"] = df[col_first].fillna("").astype(str).str.strip() + " " + df[col_last].fillna("").astype(str).str.strip()
        elif col_first:
            df["Name"] = df[col_first].astype(str)
        else:
            df["Name"] = "Unknown"

    # Parse date
    if col_date:
        df["_date"] = pd.to_datetime(df[col_date], errors="coerce")
    else:
        df["_date"] = pd.NaT

    # Detect "paid" rows
    def is_paid(row):
        s = str(row.get(col_status, "")).lower() if col_status else ""
        if any(x in s for x in ["success", "paid", "active"]):
            return True
        if col_price and pd.notna(row.get(col_price)):
            return True
        return False

    # Price
    if col_price:
        df["_price"] = pd.to_numeric(df[col_price], errors="coerce").fillna(0.0)
    else:
        df["_price"] = 0.0

    df["_province"] = df[col_province] if col_province else None
    df["_suburb"] = df[col_suburb] if col_suburb else None
    df["_street"] = df[col_street] if col_street else None

    df["_paid"] = df.apply(is_paid, axis=1)
    return df

# ----------------------------
# Metrics calculation
# ----------------------------
def compute_rep_metrics(df):
    # filter out rows with missing name
    df = df.copy()
    df["Name"] = df["Name"].fillna("Unknown")

    # base groupings
    reps = df["Name"].unique()

    # Precompute time windows
    now = pd.Timestamp.now()
    last_7 = now - pd.Timedelta(days=7)
    last_30 = now - pd.Timedelta(days=30)

    # aggregated counts per rep
    agg = []
    for name in reps:
        r = df[df["Name"] == name]
        total_signups = len(r)
        paid_rows = r[r["_paid"]]
        paid_sales = len(paid_rows)
        # potential earning: signups * avg price (use average price across rep rows)
        avg_price = r["_price"].replace(0, pd.NA).dropna().mean()
        if np.isnan(avg_price) or avg_price is None:
            avg_price = 0.0
        potential_earnings = total_signups * avg_price
        earnings = paid_rows["_price"].sum()

        # first sale date (earliest _date where paid or created)
        first_sale = r["_date"].min() if r["_date"].notna().any() else pd.NaT
        first_paid = paid_rows["_date"].min() if paid_rows["_date"].notna().any() else pd.NaT
        first_event = first_paid if pd.notna(first_paid) else first_sale

        # active days: days since first_event or since start of CSV
        if pd.isna(first_event):
            days_active = 1
        else:
            days_active = max(1, (pd.Timestamp.now() - first_event).days)

        sales_per_day = round(paid_sales / days_active, 2)

        # suburbs / streets visited list (unique)
        suburbs = r[["_suburb"]].dropna().astype(str).squeeze()
        if isinstance(suburbs, pd.Series):
            suburbs_list = suburbs.unique().tolist()
        else:
            suburbs_list = [str(suburbs)] if pd.notna(suburbs) else []

        streets = r[["_street"]].dropna().astype(str).squeeze()
        if isinstance(streets, pd.Series):
            streets_list = streets.unique().tolist()
        else:
            streets_list = [str(streets)] if pd.notna(streets) else []

        # ranks: compute later using earnings
        # weekly & monthly paid sales
        weekly_paid = r[(r["_date"] >= last_7) & (r["_paid"]) & r["_date"].notna()]
        monthly_paid = r[(r["_date"] >= last_30) & (r["_paid"]) & r["_date"].notna()]
        weekly_count = len(weekly_paid)
        monthly_count = len(monthly_paid)

        # province (most common)
        prov = None
        if "_province" in r.columns:
            prov = r["_province"].mode().iloc[0] if r["_province"].notna().any() else None

        agg.append({
            "Name": name,
            "Total_SignUps": int(total_signups),
            "Paid_Sales": int(paid_sales),
            "Potential_Earnings_ZAR": float(potential_earnings),
            "Earnings_ZAR": float(earnings),
            "Sales_Per_Day": sales_per_day,
            "Suburbs_Visited": ", ".join(suburbs_list[:6]) if suburbs_list else "",
            "Streets_Visited": ", ".join(streets_list[:8]) if streets_list else "",
            "First_Event": first_event,
            "Days_Active": int(days_active),
            "Weekly_Paid": int(weekly_count),
            "Monthly_Paid": int(monthly_count),
            "Province": prov
        })

    agg_df = pd.DataFrame(agg)

    # Ranking: national by Earnings_ZAR descending
    agg_df["Rank_Country"] = agg_df["Earnings_ZAR"].rank(ascending=False, method="min").astype(int)

    # Province rank
    agg_df["Rank_Province"] = agg_df.groupby("Province")["Earnings_ZAR"].rank(ascending=False, method="min").astype("Int64")

    # Week/Month rank (by weekly_paid, monthly_paid)
    agg_df["Rank_Week"] = agg_df["Weekly_Paid"].rank(ascending=False, method="min").astype(int)
    agg_df["Rank_Month"] = agg_df["Monthly_Paid"].rank(ascending=False, method="min").astype(int)

    # sort
    agg_df.sort_values("Rank_Country", inplace=True)

    return agg_df

# ----------------------------
# Build Dash app UI
# ----------------------------
app = Dash(__name__)
server = app.server

app.layout = html.Div([
    dcc.Interval(id="interval-refresh", interval=60*1000, n_intervals=0),  # refresh every 60s
    html.Div([
        html.H2("Sales Rep Performance", style={"color":"#F28705", "textAlign":"center"}),
        html.Div(id="last-updated", style={"textAlign":"center", "color":"#ddd", "fontSize":"12px"})
    ]),
  html.Div([
    dcc.Input(
        id="rep-search",
        type="text",
        placeholder="Search salesperson...",
        style={"width":"70%", "padding":"8px", "borderRadius":"6px", "border":"1px solid #444"}
    ),
    html.Div(id="rep-search-results", style={"marginTop":"5px", "color":"#aaa", "fontSize":"12px"})
], style={"padding":"10px 0", "textAlign":"center"}),

    html.Div(id="profile-area", style={"maxWidth":"480px", "margin":"10px auto"}),
    html.Div(id="tiles-area", style={"maxWidth":"480px", "margin":"10px auto"}),
    html.Div(id="trend-area", style={"maxWidth":"720px", "margin":"20px auto"})
], style={"backgroundColor":"#0B132B", "color":"#E0E0E0", "minHeight":"100vh", "padding":"12px"})

# ----------------------------
# Callbacks
# ----------------------------
@app.callback(
    [
        Output("rep-search-results", "children"),
        Output("profile-area", "children"),
        Output("tiles-area", "children"),
        Output("trend-area", "children"),
    ],
    [
        Input("rep-search", "value"),
        Input("interval-refresh", "n_intervals")
    ]
)
def update_view(search_value, n):
    df = load_and_prepare(CSV_PATH)
    agg = compute_rep_metrics(df)

    # --- Handle search ---
    if not search_value:
        return "Type a name to search.", "", "", ""

    # case-insensitive fuzzy search
    search_value = search_value.strip().lower()
    matches = agg[agg["Name"].str.lower().str.contains(search_value)]

    if matches.empty:
        return "No matching reps found.", "", "", ""

    # auto-select the first match
    person = matches.iloc[0]

    # -------------------------
    # HEADER CARD
    # -------------------------
    header = html.Div([
        html.Div(person["Name"], style={
            "fontSize": "20px",
            "fontWeight": "700",
            "color": "#fff",
            "textAlign": "center"
        }),
        html.Div(f"Rank #{person['Rank_Country']} Nationwide", style={
            "textAlign": "center",
            "color": "#ddd",
            "fontSize": "12px"
        })
    ], style={
        "backgroundColor": "#07203a",
        "padding": "12px",
        "borderRadius": "10px",
        "marginBottom": "12px"
    })

    # -------------------------
    # TILE HELPER FUNCTION
    # -------------------------
    def tile(title, value, bg="#2D6AA1", subtitle=None):
        return html.Div([
            html.Div(title, style={"fontSize": "12px", "color": "#e6eef8"}),
            html.Div(value, style={"fontSize": "26px", "fontWeight": "700", "color": "#fff"}),
            html.Div(subtitle or "", style={"fontSize": "11px", "color": "#d8eaf6"})
        ], style={
            "backgroundColor": bg,
            "borderRadius": "12px",
            "padding": "12px",
            "width": "48%",
            "marginBottom": "8px"
        })

    # -------------------------
    # 2×3 TILE GRID
    # -------------------------
    tiles = html.Div([
        html.Div([
            tile("SIGN UPS", person["Total_SignUps"], bg="#587fd1"),
            tile("PAID SALES", person["Paid_Sales"], bg="#294f8a"),
        ], style={"display": "flex", "justifyContent": "space-between"}),

        html.Div([
            tile("POTENTIAL (ZAR)", f"R {int(person['Potential_Earnings_ZAR']):,}", bg="#f29b66"),
            tile("EARNINGS (ZAR)", f"R {int(person['Earnings_ZAR']):,}", bg="#4fbf7a"),
        ], style={"display": "flex", "justifyContent": "space-between"}),

        html.Div([
            tile("SALES / DAY", person["Sales_Per_Day"], bg="#6f5fa0"),
            tile("LOCATIONS", person["Suburbs_Visited"], bg="#6b6b6b"),
        ], style={"display": "flex", "justifyContent": "space-between"}),
    ], style={"maxWidth": "500px", "margin": "0 auto"})

    # -------------------------
    # TREND CHART
    # -------------------------
    df_person = df[df["Name"] == person["Name"]]

    if "_date" in df_person.columns and df_person["_date"].notna().any():
        df_paid = df_person[df_person["_paid"] & df_person["_date"].notna()].copy()

        if not df_paid.empty:
            df_paid["_period"] = df_paid["_date"].dt.to_period("M").dt.to_timestamp()
            monthly = df_paid.groupby("_period").size().reset_index(name="paid_sales")
            fig = px.line(monthly, x="_period", y="paid_sales",
                          title="Monthly Paid Sales", markers=True)
        else:
            fig = px.line(pd.DataFrame({"_period": [pd.Timestamp.now()], "paid_sales": [0]}),
                          x="_period", y="paid_sales", title="Monthly Paid Sales")
    else:
        fig = px.line(pd.DataFrame({"_period": [pd.Timestamp.now()], "paid_sales": [0]}),
                      x="_period", y="paid_sales", title="Monthly Paid Sales")

    fig.update_layout(paper_bgcolor="#0B132B",
                      plot_bgcolor="#0B132B",
                      font_color="#E0E0E0")

    trend = dcc.Graph(figure=fig, style={"backgroundColor": "#0B132B"})

    return f"Found {len(matches)} matching reps", header, tiles, trend


# ----------------------------
# Run
# ----------------------------
if __name__ == "__main__":
    app.run(debug=True)
