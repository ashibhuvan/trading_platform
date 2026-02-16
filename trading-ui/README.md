# Trading UI

Grid-based trading viewserver with draggable, resizable components.

## Quick Start

### Local Development
```bash
npm install
npm run dev
```
Open http://localhost:3000

### Production Build
```bash
npm run build
npm start
```

### Docker
```bash
# Build and run
docker build -t trading-ui .
docker run -p 3000:3000 trading-ui

# Or with docker-compose
docker-compose up
```

## Cloud Deployment

### AWS ECS / Fargate
```bash
# Build and push to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker build -t trading-ui .
docker tag trading-ui:latest <account>.dkr.ecr.<region>.amazonaws.com/trading-ui:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/trading-ui:latest
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trading-ui
spec:
  replicas: 2
  selector:
    matchLabels:
      app: trading-ui
  template:
    metadata:
      labels:
        app: trading-ui
    spec:
      containers:
      - name: trading-ui
        image: trading-ui:latest
        ports:
        - containerPort: 3000
        env:
        - name: WS_ENDPOINT
          value: "ws://price-feed-service:8080/feed"
        livenessProbe:
          httpGet:
            path: /health
            port: 3000
          initialDelaySeconds: 5
          periodSeconds: 30
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "200m"
---
apiVersion: v1
kind: Service
metadata:
  name: trading-ui
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 3000
  selector:
    app: trading-ui
```

### Google Cloud Run
```bash
gcloud run deploy trading-ui \
  --image gcr.io/PROJECT/trading-ui \
  --port 3000 \
  --allow-unauthenticated
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Browser                           │
│  ┌───────────────────────────────────────────────┐ │
│  │              React TradingUI                   │ │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐         │ │
│  │  │  Chart  │ │ Ladder  │ │ Orders  │  ...    │ │
│  │  └─────────┘ └─────────┘ └─────────┘         │ │
│  └───────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
                         │
                         │ HTTP / WebSocket
                         ▼
┌─────────────────────────────────────────────────────┐
│              Express Server (Node.js)                │
│  • Serves static React build                         │
│  • /health endpoint for k8s                          │
│  • /api/config for runtime config                    │
│  • Future: WebSocket proxy to price feed             │
└─────────────────────────────────────────────────────┘
                         │
                         │ WebSocket
                         ▼
┌─────────────────────────────────────────────────────┐
│              Price Feed Service                      │
│  (separate deployment)                               │
└─────────────────────────────────────────────────────┘
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| PORT | 3000 | Server port |
| WS_ENDPOINT | ws://localhost:8080/feed | Price feed WebSocket URL |

## Features

- [x] Grid-based layout (12×8)
- [x] Drag to move components
- [x] Resize from corner handle
- [x] Collision detection
- [x] Auto-placement
- [x] Docker + k8s ready
- [ ] Persist layout to localStorage
- [ ] Connect to price feed WebSocket
- [ ] Real chart/ladder components
