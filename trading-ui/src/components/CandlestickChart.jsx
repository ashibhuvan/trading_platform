import React, { useState, useEffect, useRef, useCallback } from 'react';

/**
 * Candlestick Chart Component
 * 
 * Features:
 * - Candlestick rendering (OHLCV)
 * - Volume bars
 * - Crosshair with price/time
 * - Zoom (mouse wheel)
 * - Pan (click + drag)
 * - Auto-scale Y axis
 * - Current price line
 */

// Generate mock OHLCV data
const generateMockData = (numBars = 100) => {
  const data = [];
  let price = 4500;
  const now = Date.now();
  const interval = 60000; // 1 minute

  for (let i = 0; i < numBars; i++) {
    const volatility = 0.002;
    const change = price * volatility * (Math.random() - 0.5) * 2;
    const open = price;
    const close = price + change;
    const high = Math.max(open, close) + Math.abs(change) * Math.random();
    const low = Math.min(open, close) - Math.abs(change) * Math.random();
    const volume = Math.floor(1000 + Math.random() * 5000);

    data.push({
      time: now - (numBars - i) * interval,
      open: Math.round(open * 100) / 100,
      high: Math.round(high * 100) / 100,
      low: Math.round(low * 100) / 100,
      close: Math.round(close * 100) / 100,
      volume,
    });

    price = close;
  }
  return data;
};

// Format price
const formatPrice = (price) => price.toFixed(2);

// Format time
const formatTime = (timestamp) => {
  const date = new Date(timestamp);
  return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
};

const formatDate = (timestamp) => {
  const date = new Date(timestamp);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const CandlestickChart = ({ width = 600, height = 400, symbol = 'ES' }) => {
  const canvasRef = useRef(null);
  const [data, setData] = useState([]);
  const [viewState, setViewState] = useState({
    startIndex: 0,
    barsVisible: 50,
  });
  const [crosshair, setCrosshair] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, index: 0 });

  // Chart layout
  const PADDING = { top: 20, right: 70, bottom: 50, left: 10 };
  const VOLUME_HEIGHT_RATIO = 0.2;

  // Colors
  const COLORS = {
    background: '#0a0a0f',
    grid: 'rgba(255, 255, 255, 0.06)',
    text: 'rgba(255, 255, 255, 0.6)',
    textBright: 'rgba(255, 255, 255, 0.9)',
    bullish: '#00c853',      // Green
    bearish: '#ff1744',      // Red
    bullishVolume: 'rgba(0, 200, 83, 0.4)',
    bearishVolume: 'rgba(255, 23, 68, 0.4)',
    crosshair: 'rgba(255, 255, 255, 0.4)',
    currentPrice: '#2196f3',
  };

  // Initialize data
  useEffect(() => {
    setData(generateMockData(200));
  }, []);

  // Simulate live updates
  useEffect(() => {
    const interval = setInterval(() => {
      setData(prev => {
        if (prev.length === 0) return prev;
        
        const newData = [...prev];
        const last = { ...newData[newData.length - 1] };
        
        // Random price movement
        const change = last.close * 0.0005 * (Math.random() - 0.5) * 2;
        last.close = Math.round((last.close + change) * 100) / 100;
        last.high = Math.max(last.high, last.close);
        last.low = Math.min(last.low, last.close);
        last.volume += Math.floor(Math.random() * 50);
        
        newData[newData.length - 1] = last;
        return newData;
      });
    }, 500);

    return () => clearInterval(interval);
  }, []);

  // Calculate visible data range
  const getVisibleData = useCallback(() => {
    const { startIndex, barsVisible } = viewState;
    const endIndex = Math.min(startIndex + barsVisible, data.length);
    return data.slice(startIndex, endIndex);
  }, [data, viewState]);

  // Calculate Y scale
  const getYScale = useCallback((visibleData, chartHeight) => {
    if (visibleData.length === 0) return { min: 0, max: 100, scale: 1 };
    
    let min = Math.min(...visibleData.map(d => d.low));
    let max = Math.max(...visibleData.map(d => d.high));
    
    const padding = (max - min) * 0.1;
    min -= padding;
    max += padding;
    
    const scale = chartHeight / (max - min);
    return { min, max, scale };
  }, []);

  // Draw chart
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || data.length === 0) return;

    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);

    const chartWidth = width - PADDING.left - PADDING.right;
    const chartHeight = (height - PADDING.top - PADDING.bottom) * (1 - VOLUME_HEIGHT_RATIO);
    const volumeHeight = (height - PADDING.top - PADDING.bottom) * VOLUME_HEIGHT_RATIO;

    // Clear
    ctx.fillStyle = COLORS.background;
    ctx.fillRect(0, 0, width, height);

    const visibleData = getVisibleData();
    if (visibleData.length === 0) return;

    const { min, max, scale } = getYScale(visibleData, chartHeight);
    const barWidth = chartWidth / viewState.barsVisible;
    const candleWidth = Math.max(1, barWidth * 0.7);

    // Y-axis price to pixel
    const priceToY = (price) => PADDING.top + chartHeight - (price - min) * scale;

    // Draw grid lines
    ctx.strokeStyle = COLORS.grid;
    ctx.lineWidth = 1;
    
    // Horizontal grid
    const priceStep = (max - min) / 5;
    for (let i = 0; i <= 5; i++) {
      const price = min + priceStep * i;
      const y = priceToY(price);
      
      ctx.beginPath();
      ctx.moveTo(PADDING.left, y);
      ctx.lineTo(width - PADDING.right, y);
      ctx.stroke();
      
      // Price labels
      ctx.fillStyle = COLORS.text;
      ctx.font = '10px monospace';
      ctx.textAlign = 'left';
      ctx.fillText(formatPrice(price), width - PADDING.right + 5, y + 3);
    }

    // Vertical grid (time)
    const timeStep = Math.ceil(viewState.barsVisible / 6);
    for (let i = 0; i < visibleData.length; i += timeStep) {
      const x = PADDING.left + i * barWidth + barWidth / 2;
      
      ctx.strokeStyle = COLORS.grid;
      ctx.beginPath();
      ctx.moveTo(x, PADDING.top);
      ctx.lineTo(x, height - PADDING.bottom);
      ctx.stroke();
      
      // Time labels
      ctx.fillStyle = COLORS.text;
      ctx.textAlign = 'center';
      ctx.fillText(formatTime(visibleData[i].time), x, height - PADDING.bottom + 15);
      ctx.fillText(formatDate(visibleData[i].time), x, height - PADDING.bottom + 28);
    }

    // Draw volume bars
    const maxVolume = Math.max(...visibleData.map(d => d.volume));
    const volumeScale = volumeHeight / maxVolume;
    const volumeTop = PADDING.top + chartHeight;

    visibleData.forEach((bar, i) => {
      const x = PADDING.left + i * barWidth + (barWidth - candleWidth) / 2;
      const volHeight = bar.volume * volumeScale;
      const isBullish = bar.close >= bar.open;
      
      ctx.fillStyle = isBullish ? COLORS.bullishVolume : COLORS.bearishVolume;
      ctx.fillRect(x, volumeTop + volumeHeight - volHeight, candleWidth, volHeight);
    });

    // Draw candles
    visibleData.forEach((bar, i) => {
      const x = PADDING.left + i * barWidth + barWidth / 2;
      const candleX = x - candleWidth / 2;
      const isBullish = bar.close >= bar.open;
      
      const color = isBullish ? COLORS.bullish : COLORS.bearish;
      
      // Wick
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, priceToY(bar.high));
      ctx.lineTo(x, priceToY(bar.low));
      ctx.stroke();
      
      // Body
      const bodyTop = priceToY(Math.max(bar.open, bar.close));
      const bodyHeight = Math.max(1, Math.abs(priceToY(bar.open) - priceToY(bar.close)));
      
      ctx.fillStyle = color;
      ctx.fillRect(candleX, bodyTop, candleWidth, bodyHeight);
    });

    // Current price line
    const currentPrice = visibleData[visibleData.length - 1].close;
    const currentY = priceToY(currentPrice);
    
    ctx.strokeStyle = COLORS.currentPrice;
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(PADDING.left, currentY);
    ctx.lineTo(width - PADDING.right, currentY);
    ctx.stroke();
    ctx.setLineDash([]);
    
    // Current price label
    ctx.fillStyle = COLORS.currentPrice;
    ctx.fillRect(width - PADDING.right, currentY - 10, PADDING.right - 5, 20);
    ctx.fillStyle = '#fff';
    ctx.font = 'bold 11px monospace';
    ctx.textAlign = 'left';
    ctx.fillText(formatPrice(currentPrice), width - PADDING.right + 5, currentY + 4);

    // Draw crosshair
    if (crosshair && !isDragging) {
      const { x, y } = crosshair;
      
      // Lines
      ctx.strokeStyle = COLORS.crosshair;
      ctx.lineWidth = 1;
      ctx.setLineDash([2, 2]);
      
      ctx.beginPath();
      ctx.moveTo(PADDING.left, y);
      ctx.lineTo(width - PADDING.right, y);
      ctx.stroke();
      
      ctx.beginPath();
      ctx.moveTo(x, PADDING.top);
      ctx.lineTo(x, height - PADDING.bottom);
      ctx.stroke();
      
      ctx.setLineDash([]);
      
      // Price label
      const hoverPrice = min + (chartHeight - (y - PADDING.top)) / scale;
      if (y < volumeTop) {
        ctx.fillStyle = 'rgba(50, 50, 60, 0.9)';
        ctx.fillRect(width - PADDING.right, y - 10, PADDING.right - 5, 20);
        ctx.fillStyle = COLORS.textBright;
        ctx.font = '11px monospace';
        ctx.fillText(formatPrice(hoverPrice), width - PADDING.right + 5, y + 4);
      }
      
      // Bar info tooltip
      const barIndex = Math.floor((x - PADDING.left) / barWidth);
      if (barIndex >= 0 && barIndex < visibleData.length) {
        const bar = visibleData[barIndex];
        const tooltipX = Math.min(x + 10, width - 140);
        const tooltipY = Math.max(PADDING.top, y - 80);
        
        ctx.fillStyle = 'rgba(20, 20, 30, 0.95)';
        ctx.fillRect(tooltipX, tooltipY, 130, 75);
        ctx.strokeStyle = 'rgba(255,255,255,0.1)';
        ctx.strokeRect(tooltipX, tooltipY, 130, 75);
        
        ctx.fillStyle = COLORS.textBright;
        ctx.font = '10px monospace';
        ctx.textAlign = 'left';
        
        const lines = [
          `Time: ${formatTime(bar.time)}`,
          `O: ${formatPrice(bar.open)}`,
          `H: ${formatPrice(bar.high)}`,
          `L: ${formatPrice(bar.low)}`,
          `C: ${formatPrice(bar.close)}`,
        ];
        
        lines.forEach((line, i) => {
          ctx.fillText(line, tooltipX + 8, tooltipY + 15 + i * 13);
        });
      }
    }

    // Symbol label
    ctx.fillStyle = COLORS.textBright;
    ctx.font = 'bold 14px sans-serif';
    ctx.textAlign = 'left';
    ctx.fillText(symbol, PADDING.left + 5, PADDING.top + 15);

  }, [data, viewState, crosshair, isDragging, width, height, symbol, getVisibleData, getYScale]);

  // Mouse handlers
  const handleMouseMove = (e) => {
    const rect = canvasRef.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    
    if (isDragging) {
      const dx = e.clientX - dragStart.x;
      const barsMoved = Math.floor(dx / ((width - PADDING.left - PADDING.right) / viewState.barsVisible));
      const newStart = Math.max(0, Math.min(data.length - viewState.barsVisible, dragStart.index - barsMoved));
      
      setViewState(prev => ({ ...prev, startIndex: newStart }));
    } else {
      setCrosshair({ x, y });
    }
  };

  const handleMouseDown = (e) => {
    setIsDragging(true);
    setDragStart({ x: e.clientX, index: viewState.startIndex });
  };

  const handleMouseUp = () => {
    setIsDragging(false);
  };

  const handleMouseLeave = () => {
    setCrosshair(null);
    setIsDragging(false);
  };

  const handleWheel = (e) => {
    e.preventDefault();
    const zoomFactor = e.deltaY > 0 ? 1.1 : 0.9;
    const newBarsVisible = Math.max(10, Math.min(200, Math.floor(viewState.barsVisible * zoomFactor)));
    
    // Keep right edge fixed when zooming
    const newStartIndex = Math.max(0, Math.min(
      data.length - newBarsVisible,
      viewState.startIndex + viewState.barsVisible - newBarsVisible
    ));
    
    setViewState({ startIndex: newStartIndex, barsVisible: newBarsVisible });
  };

  // Jump to end
  const jumpToEnd = () => {
    setViewState(prev => ({
      ...prev,
      startIndex: Math.max(0, data.length - prev.barsVisible)
    }));
  };

  return (
    <div style={{ position: 'relative', width, height }}>
      <canvas
        ref={canvasRef}
        style={{ width, height, cursor: isDragging ? 'grabbing' : 'crosshair' }}
        onMouseMove={handleMouseMove}
        onMouseDown={handleMouseDown}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseLeave}
        onWheel={handleWheel}
      />
      
      {/* Controls */}
      <div style={{
        position: 'absolute',
        top: 5,
        right: 75,
        display: 'flex',
        gap: 4,
      }}>
        <button
          onClick={() => setViewState(prev => ({ ...prev, barsVisible: Math.min(200, prev.barsVisible + 20) }))}
          style={buttonStyle}
          title="Zoom Out"
        >
          −
        </button>
        <button
          onClick={() => setViewState(prev => ({ ...prev, barsVisible: Math.max(10, prev.barsVisible - 20) }))}
          style={buttonStyle}
          title="Zoom In"
        >
          +
        </button>
        <button
          onClick={jumpToEnd}
          style={buttonStyle}
          title="Jump to Latest"
        >
          ▶|
        </button>
      </div>
    </div>
  );
};

const buttonStyle = {
  background: 'rgba(255,255,255,0.1)',
  border: '1px solid rgba(255,255,255,0.2)',
  color: '#fff',
  width: 24,
  height: 20,
  fontSize: 12,
  cursor: 'pointer',
  borderRadius: 3,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
};

export default CandlestickChart;
