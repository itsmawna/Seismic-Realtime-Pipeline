import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import threading
from datetime import datetime
from collections import deque, Counter
import numpy as np

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'RawSeismicData'
MAX_EVENTS = 200

# In-memory storage
events_data = deque(maxlen=MAX_EVENTS)
events_lock = threading.Lock()

def kafka_consumer_thread():
    """Thread to continuously consume Kafka messages"""
    print("[INFO] Connecting to Kafka...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("[SUCCESS] Connected to Kafka, listening for events...")

    processed_ids = set()

    for message in consumer:
        try:
            data = message.value
            if data.get('mag') and data['mag'] >= 2.0:
                event_id = f"{data.get('time')}_{data.get('lat')}_{data.get('lon')}"
                if event_id not in processed_ids:
                    processed_ids.add(event_id)

                    # Alert detection
                    is_alert = False
                    alert_level = 'normal'
                    alert_reasons = []

                    if data.get('mag') >= 6.0:
                        is_alert = True
                        alert_level = 'critical'
                        alert_reasons.append(f"CRITICAL MAGNITUDE ({data['mag']})")
                    elif data.get('mag') >= 5.0:
                        is_alert = True
                        alert_level = 'high'
                        alert_reasons.append(f"HIGH MAGNITUDE ({data['mag']})")

                    if data.get('depth') and data['depth'] < 20:
                        is_alert = True
                        if alert_level == 'normal':
                            alert_level = 'medium'
                        alert_reasons.append(f"SHALLOW DEPTH ({data['depth']} km)")

                    event = {
                        'id': event_id,
                        'magnitude': data.get('mag'),
                        'region': data.get('flynn_region', 'Unknown'),
                        'time': data.get('time', ''),
                        'latitude': data.get('lat'),
                        'longitude': data.get('lon'),
                        'depth': data.get('depth', 0),
                        'magtype': data.get('magtype', ''),
                        'is_alert': is_alert,
                        'alert_level': alert_level,
                        'alert_reasons': alert_reasons,
                        'processed_at': datetime.now().isoformat(),
                        'timestamp': datetime.now()
                    }

                    with events_lock:
                        events_data.append(event)

                    status = "[ALERT]" if is_alert else "[EVENT]"
                    print(f"{status} Magnitude {data.get('mag')} - {data.get('flynn_region')}")

        except Exception as e:
            print(f"[ERROR] {e}")


def create_dashboard():
    """Create professional 3x3 dashboard"""
    with events_lock:
        events = list(events_data)

    if not events:
        fig = go.Figure()
        fig.add_annotation(
            text="Waiting for seismic data...<br>Ensure producer and consumer are running",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color='#94a3b8', family='Arial')
        )
        fig.update_layout(
            height=900,
            plot_bgcolor='#0f172a',
            paper_bgcolor='#0f172a',
            margin=dict(l=20, r=20, t=20, b=20)
        )
        return fig

    # Data preparation
    lats = [e['latitude'] for e in events if e['latitude'] is not None]
    lons = [e['longitude'] for e in events if e['longitude'] is not None]
    mags = [e['magnitude'] for e in events if e['magnitude'] is not None]
    regions = [e['region'] for e in events]
    depths = [e['depth'] for e in events]
    alert_levels = [e['alert_level'] for e in events]

    color_map = {'critical': '#ef4444', 'high': '#f97316', 'medium': '#f59e0b', 'normal': '#3b82f6'}
    colors = [color_map[level] for level in alert_levels]

    # Count stats
    critical_count = sum(1 for l in alert_levels if l=='critical')
    high_count = sum(1 for l in alert_levels if l=='high')
    medium_count = sum(1 for l in alert_levels if l=='medium')
    normal_count = len(events) - critical_count - high_count - medium_count

    # Create 3x3 subplots
    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=(
            'Global Seismic Activity',
            'Magnitude Distribution',
            'Alert Status',
            'Regional Activity (Top 10)',
            'Depth Analysis',
            'Magnitude Timeline',
            '',
            'Depth vs Magnitude',
            'Event Classification'
        ),
        specs=[
            [{"type": "scattergeo", "rowspan": 2, "colspan": 1}, {"type": "bar"}, {"type": "indicator"}],
            [None, {"type": "bar"}, {"type": "scatter"}],
            [{"type": "scatter"}, {"type": "scatter"}, {"type": "pie"}]
        ],
        vertical_spacing=0.10,
        horizontal_spacing=0.10,
        row_heights=[0.35, 0.35, 0.30]
    )

    # Global map - CORRECTION: spécifier explicitement row et col
    sizes = [max(m*3, 6) for m in mags]
    fig.add_trace(
        go.Scattergeo(
            lon=lons, lat=lats, mode='markers',
            marker=dict(size=sizes, color=colors, opacity=0.7, line=dict(width=1, color='white')),
            text=[f"<b>{r}</b><br>Mag: {m:.1f}<br>Depth: {d:.0f} km<br>Alert: {l.upper()}" 
                  for m,r,d,l in zip(mags,regions,depths,alert_levels)],
            hovertemplate='%{text}<extra></extra>',
            name='Events',
            showlegend=False
        ),
        row=1, col=1
    )

    # Magnitude histogram
    bins = np.arange(2, 10, 0.4)
    hist, bin_edges = np.histogram(mags, bins=bins)
    bin_centers = [(bin_edges[i] + bin_edges[i+1])/2 for i in range(len(bin_edges)-1)]
    fig.add_trace(
        go.Bar(
            x=[f"{b:.1f}" for b in bin_centers][::2],
            y=hist[::2],
            marker=dict(color='#3b82f6', line=dict(color='#1e293b', width=1)),
            showlegend=False
        ),
        row=1, col=2
    )

    # Alert gauge
    alert_pct = ((critical_count + high_count + medium_count)/len(events)*100) if events else 0
    gauge_color = '#ef4444' if alert_pct>30 else '#f59e0b' if alert_pct>15 else '#22c55e'
    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=alert_pct,
            title={'text':"Alert Status",'font':{'size':16,'color':'#e2e8f0'}},
            number={'suffix':"%",'font':{'size':32,'color':gauge_color}},
            gauge={
                'axis':{'range':[None,100],'tickcolor':'#64748b','tickfont':{'color':'#94a3b8'}},
                'bar':{'color':gauge_color,'thickness':0.75},
                'bgcolor':"#1e293b",
                'borderwidth':2,
                'bordercolor':"#334155",
                'steps':[{'range':[0,15],'color':'#0f172a'},
                        {'range':[15,30],'color':'#1e293b'},
                        {'range':[30,100],'color':'#334155'}]
            }
        ),
        row=1, col=3
    )

    # Regional activity
    region_counts = Counter(regions)
    top_regions = region_counts.most_common(10)
    region_names = [r[:30] for r,_ in top_regions]
    region_values = [c for _,c in top_regions]
    fig.add_trace(
        go.Bar(
            y=region_names[::-1],
            x=region_values[::-1],
            orientation='h',
            marker=dict(color='#8b5cf6', line=dict(color='#1e293b', width=1)),
            text=region_values[::-1],
            textposition='auto',
            textfont=dict(color='white'),
            showlegend=False
        ),
        row=2, col=2
    )

    # Depth analysis
    depth_data = depths[-50:] if len(depths)>50 else depths
    fig.add_trace(
        go.Scatter(
            x=list(range(len(depth_data))),
            y=depth_data,
            mode='lines+markers',
            marker=dict(size=6, color='#06b6d4', line=dict(width=1,color='white')),
            line=dict(color='#06b6d4', width=2),
            showlegend=False
        ),
        row=2, col=3
    )

    # Magnitude timeline
    mag_data = mags[-50:] if len(mags)>50 else mags
    color_data = colors[-50:] if len(colors)>50 else colors
    fig.add_trace(
        go.Scatter(
            x=list(range(len(mag_data))),
            y=mag_data,
            mode='lines+markers',
            marker=dict(size=8, color=color_data, line=dict(width=1,color='white')),
            line=dict(color='rgba(168,85,247,0.4)', width=2),
            fill='tozeroy',
            fillcolor='rgba(168,85,247,0.1)',
            showlegend=False
        ),
        row=3, col=1
    )

    # Depth vs magnitude
    fig.add_trace(
        go.Scatter(
            x=mags, y=depths,
            mode='markers',
            marker=dict(size=10, color=colors, opacity=0.7, line=dict(width=1,color='white')),
            text=regions,
            hovertemplate='<b>%{text}</b><br>Mag: %{x:.1f}<br>Depth: %{y:.0f} km<extra></extra>',
            showlegend=False
        ),
        row=3, col=2
    )

    # Event classification pie
    class_labels = ['Critical (≥6.0)','High (5.0-5.9)','Medium Alert','Normal']
    class_values = [critical_count, high_count, medium_count, normal_count]
    class_colors = ['#ef4444','#f97316','#f59e0b','#3b82f6']

    filtered_labels = [l for l,v in zip(class_labels,class_values) if v>0]
    filtered_values = [v for v in class_values if v>0]
    filtered_colors = [c for c,v in zip(class_colors,class_values) if v>0]

    fig.add_trace(
        go.Pie(
            labels=filtered_labels,
            values=filtered_values,
            marker=dict(colors=filtered_colors, line=dict(color='white', width=2)),
            textinfo='label+percent',
            textfont=dict(size=10,color='white'),
            hole=0.3,
            showlegend=True
        ),
        row=3, col=3
    )

    # Styling - CORRECTION: update_geos pour la carte globale
    fig.update_geos(
        projection_type="natural earth",
        showland=True, landcolor="#1e293b",
        showocean=True, oceancolor="#0f172a",
        showcountries=True, countrycolor="#334155",
        showcoastlines=True, coastlinecolor="#475569",
        showframe=False,
        bgcolor="#0f172a",
        row=1, col=1
    )

    fig.update_xaxes(showgrid=True, gridcolor='rgba(148,163,184,0.1)', zeroline=False, color='#94a3b8', showline=True, linecolor='#334155')
    fig.update_yaxes(showgrid=True, gridcolor='rgba(148,163,184,0.1)', zeroline=False, color='#94a3b8', showline=True, linecolor='#334155')

    for annotation in fig['layout']['annotations']:
        annotation['font'] = dict(size=12, color='#e2e8f0', family='Arial')

    fig.update_layout(
        height=950,
        plot_bgcolor='#0f172a',
        paper_bgcolor='#0f172a',
        font=dict(color='#e2e8f0', size=10, family='Arial'),
        margin=dict(l=40,r=40,t=30,b=40),
        hovermode='closest',
        showlegend=False
    )

    return fig


def main():
    import dash
    from dash import dcc, html
    from dash.dependencies import Input, Output

    # Start Kafka consumer
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()

    # IMPORTANT: Suppress callback exceptions to avoid conflicts
    app = dash.Dash(__name__, suppress_callback_exceptions=True)

    # CORRECTION: Layout structuré avec des statistiques organisées
    app.layout = html.Div([
        # Header
        html.Div([
            html.H1("Professional Seismic Monitoring System", 
                   style={'color':'#e2e8f0', 'margin':'0', 'fontSize':'28px'}),
            html.Div(id='live-time', 
                    style={'color':'#94a3b8', 'fontFamily':'monospace', 'fontSize':'16px', 'marginTop':'5px'})
        ], style={'padding':'20px', 'borderBottom':'2px solid #334155'}),
        
        # Statistics Cards
        html.Div([
            html.Div([
                html.Div("📊 Total Events", style={'color':'#94a3b8', 'fontSize':'12px', 'marginBottom':'5px'}),
                html.Div(id='stat-total', style={'color':'#3b82f6', 'fontSize':'32px', 'fontWeight':'bold'})
            ], style={'backgroundColor':'#1e293b', 'padding':'20px', 'borderRadius':'10px', 'border':'1px solid #334155', 'flex':'1'}),
            
            html.Div([
                html.Div("🚨 Critical Alerts", style={'color':'#94a3b8', 'fontSize':'12px', 'marginBottom':'5px'}),
                html.Div(id='stat-critical', style={'color':'#ef4444', 'fontSize':'32px', 'fontWeight':'bold'})
            ], style={'backgroundColor':'#1e293b', 'padding':'20px', 'borderRadius':'10px', 'border':'1px solid #334155', 'flex':'1'}),
            
            html.Div([
                html.Div("📈 Avg Magnitude", style={'color':'#94a3b8', 'fontSize':'12px', 'marginBottom':'5px'}),
                html.Div(id='stat-avg-mag', style={'color':'#f59e0b', 'fontSize':'32px', 'fontWeight':'bold'})
            ], style={'backgroundColor':'#1e293b', 'padding':'20px', 'borderRadius':'10px', 'border':'1px solid #334155', 'flex':'1'}),
            
            html.Div([
                html.Div("⬇️ Avg Depth", style={'color':'#94a3b8', 'fontSize':'12px', 'marginBottom':'5px'}),
                html.Div(id='stat-avg-depth', style={'color':'#06b6d4', 'fontSize':'32px', 'fontWeight':'bold'})
            ], style={'backgroundColor':'#1e293b', 'padding':'20px', 'borderRadius':'10px', 'border':'1px solid #334155', 'flex':'1'})
            
        ], style={'display':'flex', 'gap':'20px', 'padding':'20px'}),
        
        # Main Dashboard
        dcc.Graph(id='main-dashboard', config={'displayModeBar': False}, style={'padding':'0 20px'}),
        
        # Update Interval
        dcc.Interval(id='interval-component', interval=3000, n_intervals=0)
        
    ], style={'backgroundColor':'#020617', 'minHeight':'100vh', 'fontFamily':'Arial'})

    def get_stats():
        with events_lock:
            evts = list(events_data)
        if not evts:
            return {'total':0,'critical':0,'avg_mag':0,'avg_depth':0}
        return {
            'total': len(evts),
            'critical': sum(1 for e in evts if e.get('alert_level')=='critical'),
            'avg_mag': np.mean([e['magnitude'] for e in evts]),
            'avg_depth': np.mean([e['depth'] for e in evts])
        }

    @app.callback(
        [Output('main-dashboard', 'figure'),
         Output('live-time', 'children'),
         Output('stat-total', 'children'),
         Output('stat-critical', 'children'),
         Output('stat-avg-mag', 'children'),
         Output('stat-avg-depth', 'children')],
        [Input('interval-component', 'n_intervals')]
    )
    def update_dashboard(n):
        stats = get_stats()
        current_time = datetime.now().strftime(" %H:%M:%S")
        return (
            create_dashboard(),
            current_time,
            str(stats['total']),
            str(stats['critical']),
            f"{stats['avg_mag']:.2f}",
            f"{stats['avg_depth']:.0f} km"
        )

    app.run(debug=False, host='0.0.0.0', port=8050)


if __name__ == "__main__":
    main()