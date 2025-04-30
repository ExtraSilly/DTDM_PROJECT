import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
server = Flask(__name__)
CORS(server)

# Configure database
server.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://localhost/ticket_db')
server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db = SQLAlchemy(server)

# Initialize Dash app
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    url_base_pathname='/dashboard/'
)

# Define layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Ticket Sales Dashboard", className="text-center my-4"), width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='ticket-sales-graph'),
            dcc.Interval(
                id='interval-component',
                interval=60*1000,  # Update every minute
                n_intervals=0
            )
        ], width=12)
    ])
], fluid=True)

@app.callback(
    dash.Output('ticket-sales-graph', 'figure'),
    dash.Input('interval-component', 'n_intervals')
)
def update_graph(n):
    try:
        # Query data from database
        query = """
        SELECT f.film, COUNT(t.ticketcode) as ticket_count, SUM(s.total) as total_revenue
        FROM films f
        JOIN tickets t ON f.film = t.film
        JOIN sales s ON t.orderid = s.orderid
        GROUP BY f.film
        """
        
        df = pd.read_sql(query, db.engine)
        
        # Create figure
        fig = px.bar(
            df,
            x='film',
            y='ticket_count',
            title='Ticket Sales by Film',
            labels={'film': 'Film', 'ticket_count': 'Number of Tickets Sold'}
        )
        
        return fig
    except Exception as e:
        print(f"Error updating graph: {e}")
        return {}

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=int(os.getenv('PORT', 5000))) 