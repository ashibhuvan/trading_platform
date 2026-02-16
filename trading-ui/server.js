/**
 * Production server for Trading UI
 * 
 * Serves the built React app and provides API endpoints
 * for future WebSocket connections to market data feed.
 */

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Health check endpoint (for k8s/docker)
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// API routes (placeholder for future endpoints)
app.get('/api/config', (req, res) => {
  res.json({
    gridCols: 12,
    gridRows: 8,
    cellSize: 80,
    wsEndpoint: process.env.WS_ENDPOINT || 'ws://localhost:8080/feed'
  });
});

// Serve static files from the React build
app.use(express.static(path.join(__dirname, 'dist')));

// SPA fallback - serve index.html for all other routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Trading UI server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});
