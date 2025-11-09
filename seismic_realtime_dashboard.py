import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import threading
from datetime import datetime
from collections import deque, Counter

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'RawSeismicData'
MAX_EVENTS = 100  # Keep the last 100 events in memory

# In-memory storage
events_data = deque(maxlen=MAX_EVENTS)
events_lock = threading.Lock()

def kafka_consumer_thread():
    """Thread to continuously consume Kafka messages"""
    print("🔄 Connecting to Kafka...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("✅ Connected to Kafka, listening...")
    
    processed_ids = set()
    
    for message in consumer:
        try:
            data = message.value
            
            # Filter magnitude >= 2.0
            if data.get('mag') and data['mag'] >= 2.0:
                # Unique ID
                event_id = f"{data.get('time')}_{data.get('lat')}_{data.get('lon')}"
                
                if event_id not in processed_ids:
                    processed_ids.add(event_id)
                    
                    # Alert detection
                    is_alert = False
                    alert_reasons = []
                    
                    if data.get('mag') and data['mag'] >= 5.0:
                        is_alert = True
                        alert_reasons.append(f"HIGH MAGNITUDE ({data['mag']})")
                    
                    if data.get('depth') and data['depth'] < 20:
                        is_alert = True
                        alert_reasons.append(f"SHALLOW EARTHQUAKE ({data['depth']} km)")
                    
                    # Add to the list
                    event = {
                        'id': event_id,
                        'magnitude': data.get('mag'),
                        'region': data.get('flynn_region', 'Unknown'),
                        'time': data.get('time', ''),
                        'latitude': data.get('lat'),
                        'longitude': data.get('lon'),
                        'depth': data.get('depth', 0),
                        'magtype': data.get('magtype', ''),
                        'action': data.get('action', ''),
                        'is_alert': is_alert,
                        'alert_reasons': alert_reasons,
                        'processed_at': datetime.now().isoformat()
                    }
                    
                    with events_lock:
                        events_data.append(event)
                    
                    status = "🚨 ALERT" if is_alert else "✅"
                    print(f"{status} Event received: Mag {data.get('mag')} - {data.get('flynn_region')}")
        
        except Exception as e:
            print(f"❌ Error processing: {e}")

def create_dashboard():
    """Create the dashboard from in-memory data"""
    
    with events_lock:
        events = list(events_data)
    
    if not events:
        # Empty dashboard
        fig = go.Figure()
        fig.add_annotation(
            text="⏳ Waiting for seismic data...<br>Ensure the producer and processor are running",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title="🌍 REAL-TIME SEISMIC DASHBOARD",
            height=900
        )
        return fig
    
    # Prepare data
    lats = [e['latitude'] for e in events if e['latitude']]
    lons = [e['longitude'] for e in events if e['longitude']]
    mags = [e['magnitude'] for e in events if e['magnitude']]
    regions = [e['region'] for e in events if e['region']]
    times = [e['time'] for e in events if e['time']]
    depths = [e['depth'] for e in events if e['depth']]
    is_alerts = [e.get('is_alert', False) for e in events]
    
    # Colors
    colors = ['red' if alert else 'blue' for alert in is_alerts]
    alert_count = sum(is_alerts)
    
    # Create 4 subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            '🌍 World Seismic Map',
            '📊 Magnitude Distribution',
            '⬇️ Depth vs Magnitude',
            '🌐 Top 10 Regions'
        ),
        specs=[
            [{"type": "scattergeo"}, {"type": "histogram"}],
            [{"type": "scatter"}, {"type": "bar"}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    # 1️⃣ WORLD MAP
    fig.add_trace(
        go.Scattergeo(
            lon=lons,
            lat=lats,
            mode='markers',
            marker=dict(
                size=[m*3 for m in mags],
                color=colors,
                opacity=0.7,
                line=dict(width=0.5, color='white'),
                colorscale='Reds',
            ),
            text=[f"Mag: {m}<br>Region: {r}<br>Depth: {d} km" 
                  for m, r, d in zip(mags, regions, depths)],
            hovertemplate='<b>%{text}</b><extra></extra>',
            name='Seismic Events'
        ),
        row=1, col=1
    )
    
    # 2️⃣ MAGNITUDE HISTOGRAM
    fig.add_trace(
        go.Histogram(
            x=mags,
            nbinsx=20,
            marker_color='lightblue',
            name='Magnitude',
            showlegend=False
        ),
        row=1, col=2
    )
    
    # 3️⃣ DEPTH VS MAGNITUDE
    fig.add_trace(
        go.Scatter(
            x=mags,
            y=depths,
            mode='markers',
            marker=dict(
                size=10,
                color=colors,
                opacity=0.6
            ),
            text=regions,
            hovertemplate='<b>%{text}</b><br>Mag: %{x}<br>Depth: %{y} km<extra></extra>',
            name='Events',
            showlegend=False
        ),
        row=2, col=1
    )
    
    # 4️⃣ TOP 10 REGIONS
    region_counts = Counter(regions)
    top_regions = region_counts.most_common(10)
    
    fig.add_trace(
        go.Bar(
            y=[r[:30] for r, _ in top_regions],
            x=[c for _, c in top_regions],
            orientation='h',
            marker_color='lightcoral',
            name='Regions',
            showlegend=False
        ),
        row=2, col=2
    )
    
    # Map configuration
    fig.update_geos(
        projection_type="natural earth",
        showland=True,
        landcolor="lightgray",
        showocean=True,
        oceancolor="lightblue",
        showcountries=True,
        countrycolor="white"
    )
    
    # Axes
    fig.update_xaxes(title_text="Magnitude", row=1, col=2)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    
    fig.update_xaxes(title_text="Magnitude", row=2, col=1)
    fig.update_yaxes(title_text="Depth (km)", row=2, col=1)
    
    fig.update_xaxes(title_text="Number of Events", row=2, col=2)
    
    # Statistics
    avg_mag = sum(mags) / len(mags) if mags else 0
    max_mag = max(mags) if mags else 0
    
    # Layout
    fig.update_layout(
        title={
            'text': f'🌍 REAL-TIME SEISMIC DASHBOARD<br>'
                    f'<sub>Total: {len(events)} events | Alerts: {alert_count} | '
                    f'Avg Mag: {avg_mag:.2f} | Max Mag: {max_mag:.1f} | '
                    f'Updated: {datetime.now().strftime("%H:%M:%S")}</sub>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20}
        },
        height=900,
        showlegend=False,
        template='plotly_white'
    )
    
    return fig

def main():
    """Launch the interactive dashboard"""
    import dash
    from dash import dcc, html
    from dash.dependencies import Input, Output
    
    print("=" * 70)
    print("🌍 REAL-TIME SEISMIC DASHBOARD")
    print("=" * 70)
    print(f"📡 Kafka Topic: {KAFKA_TOPIC}")
    print(f"🔄 Memory: Last {MAX_EVENTS} events")
    print("🌐 Dashboard will open at: http://localhost:8050")
    print("=" * 70)
    print()
    
    # Launch Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    # Wait a bit to accumulate data
    print("⏳ Waiting for initial data (5 seconds)...")
    import time
    time.sleep(5)
    
    # Create Dash app
    app = dash.Dash(__name__)
    
    app.layout = html.Div([
        html.H1("🌍 Real-Time Seismic Dashboard", 
                style={'textAlign': 'center', 'color': '#2c3e50'}),
        dcc.Graph(id='live-graph'),
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # Update every 5 seconds
            n_intervals=0
        )
    ])
    
    @app.callback(
        Output('live-graph', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_graph(n):
        return create_dashboard()
    
    print("\n✅ Dashboard launched!")
    print("🌐 Open your browser at: http://localhost:8050")
    print("🔄 Auto-update every 5 seconds")
    print("⚠️ Press Ctrl+C to stop\n")
    
    try:
        app.run(debug=False, host='127.0.0.1', port=8050)
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("🛑 Dashboard stopped")
        print("=" * 70)

if __name__ == "__main__":
    main()
