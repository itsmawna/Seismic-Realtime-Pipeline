import { useEffect, useState } from 'react';
import io from 'socket.io-client';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import './App.css';

function App() {
  const [events, setEvents] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [stats, setStats] = useState({
    total: 0,
    alerts: 0,
    avgMagnitude: 0,
    maxMagnitude: 0
  });

  useEffect(() => {
    // Connexion au backend Socket.IO
    const socket = io('http://localhost:4000');

    socket.on('connect', () => {
      console.log('✅ Connecté au backend');
      setIsConnected(true);
    });

    socket.on('disconnect', () => {
      console.log('❌ Déconnecté du backend');
      setIsConnected(false);
    });

    socket.on('newEvent', (data) => {
      console.log('📡 Nouvel événement reçu:', data);
      
      const newEvent = {
        id: `${data.time}_${data.lat}_${data.lon}`,
        magnitude: data.mag || 0,
        region: data.flynn_region || 'Unknown',
        time: data.time || new Date().toISOString(),
        latitude: data.lat || 0,
        longitude: data.lon || 0,
        depth: data.depth || 0,
        magtype: data.magtype || 'unknown',
        action: data.action || 'create',
        timestamp: new Date(data.time || Date.now())
      };

      // Classification par niveau d'alerte
      let alert_level = 'low';
      let alert_reasons = [];

      if (newEvent.magnitude >= 7.0) {
        alert_level = 'critical';
        alert_reasons.push(`MAGNITUDE CRITIQUE (${newEvent.magnitude})`);
      } else if (newEvent.magnitude >= 6.0) {
        alert_level = 'high';
        alert_reasons.push(`FORTE MAGNITUDE (${newEvent.magnitude})`);
      } else if (newEvent.magnitude >= 5.0) {
        alert_level = 'medium';
        alert_reasons.push(`MAGNITUDE MOYENNE (${newEvent.magnitude})`);
      }

      if (newEvent.depth < 20) {
        alert_reasons.push(`FAIBLE PROFONDEUR (${newEvent.depth} km)`);
        if (alert_level === 'low' || alert_level === 'medium') {
          alert_level = 'medium';
        }
      }

      newEvent.is_alert = alert_level !== 'low';
      newEvent.alert_level = alert_level;
      newEvent.alert_reasons = alert_reasons;

      setEvents(prev => {
        const updated = [newEvent, ...prev];
        return updated.slice(0, 200); // Garder les 200 derniers
      });
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  // Calculer les statistiques
  useEffect(() => {
    if (events.length === 0) {
      setStats({ total: 0, alerts: 0, avgMagnitude: 0, maxMagnitude: 0 });
      return;
    }

    const total = events.length;
    const alerts = events.filter(e => e.is_alert).length;
    const avgMagnitude = events.reduce((sum, e) => sum + e.magnitude, 0) / total;
    const maxMagnitude = Math.max(...events.map(e => e.magnitude));

    setStats({
      total,
      alerts,
      avgMagnitude: parseFloat(avgMagnitude.toFixed(2)),
      maxMagnitude: parseFloat(maxMagnitude.toFixed(1))
    });
  }, [events]);

  const getMagnitudeClass = (mag) => {
    if (mag >= 7.0) return 'critical';
    if (mag >= 6.0) return 'high';
    if (mag >= 5.0) return 'medium';
    return 'low';
  };

  const getMarkerColor = (alertLevel) => {
    switch (alertLevel) {
      case 'critical': return '#ef4444'; // red
      case 'high': return '#f97316'; // orange
      case 'medium': return '#eab308'; // yellow
      case 'low': return '#22c55e'; // green
      default: return '#22c55e';
    }
  };

  const formatTime = (isoString) => {
    const date = new Date(isoString);
    return date.toLocaleString('fr-FR', {
      day: '2-digit',
      month: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  return (
    <div className="dashboard">
      <div className="header">
        <div className="header-top">
          <h1 className="title">🌍 Seismic Real-Time Dashboard</h1>
          <div className={`connection-status ${!isConnected ? 'disconnected' : ''}`}>
            <span className="status-dot"></span>
            <span>{isConnected ? 'Connecté' : 'Déconnecté'}</span>
          </div>
        </div>

        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Événements</div>
            <div className="stat-value">{stats.total}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Alertes</div>
            <div className="stat-value alert">{stats.alerts}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Magnitude Moyenne</div>
            <div className="stat-value">{stats.avgMagnitude}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Magnitude Max</div>
            <div className="stat-value">{stats.maxMagnitude}</div>
          </div>
        </div>
      </div>

      <div className="content">
        <div className="events-panel">
          <div className="panel-header">
            <h2 className="panel-title">Événements Récents</h2>
            <span className="event-count">{events.length} événements</span>
          </div>

          {events.length === 0 ? (
            <div className="no-events">
              En attente d'événements sismiques...
            </div>
          ) : (
            <div className="events-list">
              {events.map(event => (
                <div key={event.id} className={`event-card ${event.is_alert ? 'alert' : ''}`}>
                  <div className="event-header">
                    <div>
                      <div className="event-region">{event.region}</div>
                      <div className="alert-badges">
                        {(event.alert_level === 'critical' || event.alert_level === 'high') && (
                          <span className="alert-badge">⚠️ Alerte</span>
                        )}
                        <span className={`alert-type-badge ${event.alert_level}`}>
                          {event.alert_level === 'critical' && '🔴 CRITICAL'}
                          {event.alert_level === 'high' && '🟠 HIGH'}
                          {event.alert_level === 'medium' && '🟡 MEDIUM'}
                          {event.alert_level === 'low' && '🟢 LOW'}
                        </span>
                      </div>
                    </div>
                    <div className={`magnitude-badge ${getMagnitudeClass(event.magnitude)}`}>
                      {event.magnitude}
                    </div>
                  </div>

                  <div className="event-details">
                    <div className="detail-item">
                      <span className="detail-label">Heure:</span>
                      <span className="detail-value">{formatTime(event.time)}</span>
                    </div>
                    <div className="detail-item">
                      <span className="detail-label">Profondeur:</span>
                      <span className="detail-value">{event.depth} km</span>
                    </div>
                    <div className="detail-item">
                      <span className="detail-label">Latitude:</span>
                      <span className="detail-value">{event.latitude}°</span>
                    </div>
                    <div className="detail-item">
                      <span className="detail-label">Longitude:</span>
                      <span className="detail-value">{event.longitude}°</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="map-panel">
          <div className="panel-header">
            <h2 className="panel-title">Vue Globale</h2>
          </div>
          
          <div className="map-container">
            <MapContainer 
              center={[20, 0]} 
              zoom={2} 
              style={{ height: '400px', width: '100%', borderRadius: '8px' }}
            >
              <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />
              {events.map(event => (
                <CircleMarker
                  key={event.id}
                  center={[event.latitude, event.longitude]}
                  radius={Math.max(4, event.magnitude * 2)}
                  fillColor={getMarkerColor(event.alert_level)}
                  color={getMarkerColor(event.alert_level)}
                  weight={2}
                  opacity={0.8}
                  fillOpacity={0.6}
                >
                  <Popup>
                    <div className="popup-content">
                      <strong>{event.region}</strong><br/>
                      <div className="popup-alert-level">
                        Type: <span className={`popup-alert-${event.alert_level}`}>
                          {event.alert_level.toUpperCase()}
                        </span>
                      </div>
                      <strong>Magnitude:</strong> {event.magnitude}<br/>
                      <strong>Profondeur:</strong> {event.depth} km<br/>
                      <strong>Heure:</strong> {formatTime(event.time)}
                    </div>
                  </Popup>
                </CircleMarker>
              ))}
            </MapContainer>
          </div>

          <h3 className="recent-events-title">Derniers 5 Événements</h3>
          {events.slice(0, 5).map(event => (
            <div key={event.id} className="recent-event-item">
              <div className="recent-event-info">
                <div className="recent-event-region">{event.region}</div>
                <div className="recent-event-time">{formatTime(event.time)}</div>
              </div>
              <div className={`recent-event-mag magnitude-badge ${getMagnitudeClass(event.magnitude)}`}>
                {event.magnitude}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default App;