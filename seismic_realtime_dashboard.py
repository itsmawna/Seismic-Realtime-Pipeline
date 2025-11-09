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
MAX_EVENTS = 100  # Garde les 100 derniers événements en mémoire

# Stockage en mémoire
events_data = deque(maxlen=MAX_EVENTS)
events_lock = threading.Lock()

def kafka_consumer_thread():
    """Thread pour consommer Kafka en continu"""
    print("🔄 Connexion à Kafka...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("✅ Connecté à Kafka, en écoute...")
    
    processed_ids = set()
    
    for message in consumer:
        try:
            data = message.value
            
            # Filtre magnitude >= 2.0
            if data.get('mag') and data['mag'] >= 2.0:
                # ID unique
                event_id = f"{data.get('time')}_{data.get('lat')}_{data.get('lon')}"
                
                if event_id not in processed_ids:
                    processed_ids.add(event_id)
                    
                    # Détection d'alerte
                    is_alert = False
                    alert_reasons = []
                    
                    if data.get('mag') and data['mag'] >= 5.0:
                        is_alert = True
                        alert_reasons.append(f"FORTE MAGNITUDE ({data['mag']})")
                    
                    if data.get('depth') and data['depth'] < 20:
                        is_alert = True
                        alert_reasons.append(f"SÉISME PEU PROFOND ({data['depth']} km)")
                    
                    # Ajoute à la liste
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
                    
                    status = "🚨 ALERTE" if is_alert else "✅"
                    print(f"{status} Événement reçu: Mag {data.get('mag')} - {data.get('flynn_region')}")
        
        except Exception as e:
            print(f"❌ Erreur traitement: {e}")

def create_dashboard():
    """Crée le dashboard avec les données en mémoire"""
    
    with events_lock:
        events = list(events_data)
    
    if not events:
        # Dashboard vide
        fig = go.Figure()
        fig.add_annotation(
            text="⏳ En attente de données sismiques...<br>Assurez-vous que le producer et le processor sont lancés",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title="🌍 DASHBOARD SISMIQUE EN TEMPS RÉEL",
            height=900
        )
        return fig
    
    # Prépare les données
    lats = [e['latitude'] for e in events if e['latitude']]
    lons = [e['longitude'] for e in events if e['longitude']]
    mags = [e['magnitude'] for e in events if e['magnitude']]
    regions = [e['region'] for e in events if e['region']]
    times = [e['time'] for e in events if e['time']]
    depths = [e['depth'] for e in events if e['depth']]
    is_alerts = [e.get('is_alert', False) for e in events]
    
    # Couleurs
    colors = ['red' if alert else 'blue' for alert in is_alerts]
    alert_count = sum(is_alerts)
    
    # Crée 4 subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            '🌍 Carte Mondiale des Séismes',
            '📊 Distribution des Magnitudes',
            '⬇️ Profondeur vs Magnitude',
            '🌐 Top 10 Régions'
        ),
        specs=[
            [{"type": "scattergeo"}, {"type": "histogram"}],
            [{"type": "scatter"}, {"type": "bar"}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    # 1️⃣ CARTE MONDIALE
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
            text=[f"Mag: {m}<br>Région: {r}<br>Prof: {d} km" 
                  for m, r, d in zip(mags, regions, depths)],
            hovertemplate='<b>%{text}</b><extra></extra>',
            name='Séismes'
        ),
        row=1, col=1
    )
    
    # 2️⃣ HISTOGRAMME MAGNITUDES
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
    
    # 3️⃣ PROFONDEUR VS MAGNITUDE
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
            hovertemplate='<b>%{text}</b><br>Mag: %{x}<br>Prof: %{y} km<extra></extra>',
            name='Événements',
            showlegend=False
        ),
        row=2, col=1
    )
    
    # 4️⃣ TOP 10 RÉGIONS
    region_counts = Counter(regions)
    top_regions = region_counts.most_common(10)
    
    fig.add_trace(
        go.Bar(
            y=[r[:30] for r, _ in top_regions],  # Tronque les noms longs
            x=[c for _, c in top_regions],
            orientation='h',
            marker_color='lightcoral',
            name='Régions',
            showlegend=False
        ),
        row=2, col=2
    )
    
    # Configuration carte
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
    fig.update_yaxes(title_text="Nombre", row=1, col=2)
    
    fig.update_xaxes(title_text="Magnitude", row=2, col=1)
    fig.update_yaxes(title_text="Profondeur (km)", row=2, col=1)
    
    fig.update_xaxes(title_text="Nombre d'événements", row=2, col=2)
    
    # Statistiques
    avg_mag = sum(mags) / len(mags) if mags else 0
    max_mag = max(mags) if mags else 0
    
    # Layout
    fig.update_layout(
        title={
            'text': f'🌍 DASHBOARD SISMIQUE EN TEMPS RÉEL<br>'
                    f'<sub>Total: {len(events)} événements | Alertes: {alert_count} | '
                    f'Mag moy: {avg_mag:.2f} | Mag max: {max_mag:.1f} | '
                    f'Mis à jour: {datetime.now().strftime("%H:%M:%S")}</sub>',
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
    """Lance le dashboard interactif"""
    import dash
    from dash import dcc, html
    from dash.dependencies import Input, Output
    
    print("=" * 70)
    print("🌍 DASHBOARD SISMIQUE EN TEMPS RÉEL")
    print("=" * 70)
    print(f"📡 Kafka Topic: {KAFKA_TOPIC}")
    print(f"🔄 Mémoire: {MAX_EVENTS} derniers événements")
    print("🌐 Le dashboard s'ouvrira à: http://localhost:8050")
    print("=" * 70)
    print()
    
    # Lance le consumer Kafka dans un thread séparé
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    # Attend un peu pour accumuler des données
    print("⏳ Attente de données initiales (5 secondes)...")
    import time
    time.sleep(5)
    
    # Crée l'application Dash
    app = dash.Dash(__name__)
    
    app.layout = html.Div([
        html.H1("🌍 Dashboard Sismique en Temps Réel", 
                style={'textAlign': 'center', 'color': '#2c3e50'}),
        dcc.Graph(id='live-graph'),
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # Mise à jour toutes les 5 secondes
            n_intervals=0
        )
    ])
    
    @app.callback(
        Output('live-graph', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_graph(n):
        return create_dashboard()
    
    print("\n✅ Dashboard lancé !")
    print("🌐 Ouvrez votre navigateur à: http://localhost:8050")
    print("🔄 Mise à jour automatique toutes les 5 secondes")
    print("⚠️  Appuyez sur Ctrl+C pour arrêter\n")
    
    try:
        app.run(debug=False, host='127.0.0.1', port=8050)
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("🛑 Dashboard arrêté")
        print("=" * 70)

if __name__ == "__main__":
    main()