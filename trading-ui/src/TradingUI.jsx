import React, { useState, useCallback, useRef, useEffect } from 'react';
import CandlestickChart from './components/CandlestickChart';

// Grid configuration
const GAP = 4;

// Available symbols
const SYMBOLS = ['ES', 'NQ', 'CL', 'GC', 'EURUSD', 'GBPUSD', 'AAPL', 'MSFT', 'TSLA', 'BTC'];

// Component types that can be added
const COMPONENT_TYPES = {
  CHART: { name: 'Chart', defaultW: 6, defaultH: 5, color: '#0a0a0f' },
  PRICE_LADDER: { name: 'Price Ladder', defaultW: 2, defaultH: 5, color: '#1a1a2e' },
  POSITIONS: { name: 'Positions', defaultW: 4, defaultH: 2, color: '#1a1a2e' },
  ORDER_TICKET: { name: 'Order Ticket', defaultW: 2, defaultH: 3, color: '#0f3460' },
  WATCHLIST: { name: 'Watchlist', defaultW: 2, defaultH: 4, color: '#1a1a2e' },
  NEWS: { name: 'News', defaultW: 4, defaultH: 3, color: '#16213e' },
};

// Check if a position collides with existing components
const checkCollision = (components, newComp, excludeId = null) => {
  for (const comp of components) {
    if (comp.id === excludeId) continue;
    
    const noOverlap = 
      newComp.x + newComp.w <= comp.x ||
      comp.x + comp.w <= newComp.x ||
      newComp.y + newComp.h <= comp.y ||
      comp.y + comp.h <= newComp.y;
    
    if (!noOverlap) return true;
  }
  return false;
};

// Find next available position for a component
const findAvailablePosition = (components, w, h, gridCols, gridRows) => {
  for (let y = 0; y <= gridRows - h; y++) {
    for (let x = 0; x <= gridCols - w; x++) {
      const testComp = { x, y, w, h };
      if (!checkCollision(components, testComp)) {
        return { x, y };
      }
    }
  }
  return null;
};

// Grid cell component
const GridCell = ({ x, y, cellSize }) => (
  <div
    style={{
      position: 'absolute',
      left: x * (cellSize + GAP),
      top: y * (cellSize + GAP),
      width: cellSize,
      height: cellSize,
      backgroundColor: 'rgba(255,255,255,0.02)',
      border: '1px solid rgba(255,255,255,0.04)',
      borderRadius: 2,
    }}
  />
);

// Symbol selector dropdown
const SymbolSelector = ({ value, onChange }) => (
  <select
    value={value}
    onChange={(e) => onChange(e.target.value)}
    onClick={(e) => e.stopPropagation()}
    onMouseDown={(e) => e.stopPropagation()}
    style={{
      background: 'rgba(255,255,255,0.1)',
      border: '1px solid rgba(255,255,255,0.2)',
      borderRadius: 3,
      color: '#fff',
      fontSize: 11,
      padding: '2px 6px',
      cursor: 'pointer',
      outline: 'none',
    }}
  >
    {SYMBOLS.map(sym => (
      <option key={sym} value={sym} style={{ background: '#1a1a2e' }}>{sym}</option>
    ))}
  </select>
);

// Draggable/Resizable Component
const GridComponent = ({ 
  component, 
  onMove, 
  onResize, 
  onRemove,
  onUpdateSymbol,
  isSelected,
  onSelect,
  gridCols,
  gridRows,
  cellSize,
}) => {
  const [isDragging, setIsDragging] = useState(false);
  const [isResizing, setIsResizing] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const compRef = useRef(null);

  const style = {
    position: 'absolute',
    left: component.x * (cellSize + GAP),
    top: component.y * (cellSize + GAP),
    width: component.w * cellSize + (component.w - 1) * GAP,
    height: component.h * cellSize + (component.h - 1) * GAP,
    backgroundColor: COMPONENT_TYPES[component.type].color,
    border: isSelected ? '2px solid #4a90e2' : '1px solid rgba(255,255,255,0.1)',
    borderRadius: 4,
    cursor: isDragging ? 'grabbing' : 'grab',
    userSelect: 'none',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    boxShadow: isSelected ? '0 0 20px rgba(74, 144, 226, 0.3)' : '0 4px 12px rgba(0,0,0,0.3)',
    transition: isDragging || isResizing ? 'none' : 'all 0.15s ease',
  };

  const handleMouseDown = (e) => {
    if (e.target.closest('.no-drag')) return;
    setIsDragging(true);
    onSelect(component.id);
    const rect = compRef.current.getBoundingClientRect();
    setDragOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    });
  };

  const handleMouseMove = useCallback((e) => {
    if (!isDragging) return;
    
    const gridContainer = compRef.current.parentElement;
    const gridRect = gridContainer.getBoundingClientRect();
    
    const newX = Math.round((e.clientX - gridRect.left - dragOffset.x) / (cellSize + GAP));
    const newY = Math.round((e.clientY - gridRect.top - dragOffset.y) / (cellSize + GAP));
    
    const clampedX = Math.max(0, Math.min(gridCols - component.w, newX));
    const clampedY = Math.max(0, Math.min(gridRows - component.h, newY));
    
    if (clampedX !== component.x || clampedY !== component.y) {
      onMove(component.id, clampedX, clampedY);
    }
  }, [isDragging, dragOffset, component, onMove, gridCols, gridRows, cellSize]);

  const handleMouseUp = () => {
    setIsDragging(false);
    setIsResizing(false);
  };

  useEffect(() => {
    if (isDragging) {
      window.addEventListener('mousemove', handleMouseMove);
      window.addEventListener('mouseup', handleMouseUp);
      return () => {
        window.removeEventListener('mousemove', handleMouseMove);
        window.removeEventListener('mouseup', handleMouseUp);
      };
    }
  }, [isDragging, handleMouseMove]);

  const handleResizeMouseDown = (e) => {
    e.stopPropagation();
    setIsResizing(true);
    onSelect(component.id);
    
    const startX = e.clientX;
    const startY = e.clientY;
    const startW = component.w;
    const startH = component.h;

    const handleResizeMove = (moveEvent) => {
      const deltaX = moveEvent.clientX - startX;
      const deltaY = moveEvent.clientY - startY;
      
      const newW = Math.max(2, Math.min(gridCols - component.x, startW + Math.round(deltaX / (cellSize + GAP))));
      const newH = Math.max(2, Math.min(gridRows - component.y, startH + Math.round(deltaY / (cellSize + GAP))));
      
      if (newW !== component.w || newH !== component.h) {
        onResize(component.id, newW, newH);
      }
    };

    const handleResizeUp = () => {
      setIsResizing(false);
      window.removeEventListener('mousemove', handleResizeMove);
      window.removeEventListener('mouseup', handleResizeUp);
    };

    window.addEventListener('mousemove', handleResizeMove);
    window.addEventListener('mouseup', handleResizeUp);
  };

  const contentWidth = component.w * cellSize + (component.w - 1) * GAP - 2;
  const contentHeight = component.h * cellSize + (component.h - 1) * GAP - 36;

  return (
    <div ref={compRef} style={style} onMouseDown={handleMouseDown}>
      {/* Header */}
      <div style={{
        padding: '6px 10px',
        borderBottom: '1px solid rgba(255,255,255,0.1)',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: 'rgba(0,0,0,0.3)',
        minHeight: 32,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ color: 'rgba(255,255,255,0.5)', fontSize: 10, textTransform: 'uppercase' }}>
            {COMPONENT_TYPES[component.type].name}
          </span>
          {component.type === 'CHART' && (
            <div className="no-drag">
              <SymbolSelector 
                value={component.symbol || 'ES'} 
                onChange={(sym) => onUpdateSymbol(component.id, sym)}
              />
            </div>
          )}
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); onRemove(component.id); }}
          className="no-drag"
          style={{
            background: 'none',
            border: 'none',
            color: 'rgba(255,255,255,0.4)',
            cursor: 'pointer',
            fontSize: 18,
            padding: '0 4px',
            lineHeight: 1,
          }}
        >
          ×
        </button>
      </div>
      
      {/* Content */}
      <div style={{ 
        flex: 1, 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center',
        overflow: 'hidden',
      }}>
        {component.type === 'CHART' ? (
          <CandlestickChart 
            width={contentWidth}
            height={contentHeight}
            symbol={component.symbol || 'ES'}
          />
        ) : (
          <span style={{ color: 'rgba(255,255,255,0.2)', fontSize: 10 }}>
            {COMPONENT_TYPES[component.type].name}
          </span>
        )}
      </div>
      
      {/* Resize handle */}
      <div
        className="resize-handle no-drag"
        onMouseDown={handleResizeMouseDown}
        style={{
          position: 'absolute',
          bottom: 0,
          right: 0,
          width: 20,
          height: 20,
          cursor: 'se-resize',
          background: 'linear-gradient(135deg, transparent 50%, rgba(255,255,255,0.15) 50%)',
        }}
      />
    </div>
  );
};

// Component palette
const ComponentPalette = ({ onAdd }) => (
  <div style={{
    position: 'fixed',
    top: 10,
    right: 10,
    backgroundColor: 'rgba(13, 13, 13, 0.95)',
    border: '1px solid rgba(255,255,255,0.1)',
    borderRadius: 6,
    padding: 10,
    zIndex: 1000,
    backdropFilter: 'blur(10px)',
  }}>
    <div style={{ color: 'rgba(255,255,255,0.4)', fontSize: 9, marginBottom: 8, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
      Components
    </div>
    {Object.entries(COMPONENT_TYPES).map(([type, config]) => (
      <button
        key={type}
        onClick={() => onAdd(type)}
        style={{
          display: 'block',
          width: '100%',
          padding: '6px 10px',
          marginBottom: 3,
          backgroundColor: config.color,
          border: '1px solid rgba(255,255,255,0.08)',
          borderRadius: 3,
          color: 'rgba(255,255,255,0.8)',
          fontSize: 11,
          cursor: 'pointer',
          textAlign: 'left',
          transition: 'all 0.15s ease',
        }}
        onMouseEnter={(e) => e.target.style.borderColor = 'rgba(255,255,255,0.3)'}
        onMouseLeave={(e) => e.target.style.borderColor = 'rgba(255,255,255,0.08)'}
      >
        {config.name}
      </button>
    ))}
  </div>
);

// Main App
export default function TradingUI() {
  const [components, setComponents] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [idCounter, setIdCounter] = useState(0);
  const [gridDimensions, setGridDimensions] = useState({ cols: 16, rows: 10, cellSize: 70 });

  // Calculate grid size based on window
  useEffect(() => {
    const calculateGrid = () => {
      const padding = 20;
      const statusBarHeight = 30;
      const availableWidth = window.innerWidth - padding * 2;
      const availableHeight = window.innerHeight - statusBarHeight - padding;
      
      // Target cell size around 70-90px
      const targetCellSize = 80;
      
      const cols = Math.floor((availableWidth + GAP) / (targetCellSize + GAP));
      const rows = Math.floor((availableHeight + GAP) / (targetCellSize + GAP));
      
      // Recalculate cell size to fill space evenly
      const cellSize = Math.floor((availableWidth - (cols - 1) * GAP) / cols);
      
      setGridDimensions({ cols, rows, cellSize });
    };

    calculateGrid();
    window.addEventListener('resize', calculateGrid);
    return () => window.removeEventListener('resize', calculateGrid);
  }, []);

  const { cols: gridCols, rows: gridRows, cellSize } = gridDimensions;

  const addComponent = (type) => {
    const config = COMPONENT_TYPES[type];
    const w = Math.min(config.defaultW, gridCols);
    const h = Math.min(config.defaultH, gridRows);
    const position = findAvailablePosition(components, w, h, gridCols, gridRows);
    
    if (!position) {
      alert('No space available');
      return;
    }

    const newComponent = {
      id: idCounter,
      type,
      x: position.x,
      y: position.y,
      w,
      h,
      symbol: type === 'CHART' ? 'ES' : undefined,
    };

    setComponents([...components, newComponent]);
    setIdCounter(idCounter + 1);
    setSelectedId(newComponent.id);
  };

  const moveComponent = (id, newX, newY) => {
    setComponents(components.map(comp => {
      if (comp.id !== id) return comp;
      
      const testComp = { ...comp, x: newX, y: newY };
      if (checkCollision(components, testComp, id)) return comp;
      
      return { ...comp, x: newX, y: newY };
    }));
  };

  const resizeComponent = (id, newW, newH) => {
    setComponents(components.map(comp => {
      if (comp.id !== id) return comp;
      
      const testComp = { ...comp, w: newW, h: newH };
      if (checkCollision(components, testComp, id)) return comp;
      
      return { ...comp, w: newW, h: newH };
    }));
  };

  const updateSymbol = (id, symbol) => {
    setComponents(components.map(comp => 
      comp.id === id ? { ...comp, symbol } : comp
    ));
  };

  const removeComponent = (id) => {
    setComponents(components.filter(comp => comp.id !== id));
    if (selectedId === id) setSelectedId(null);
  };

  const handleBackgroundClick = (e) => {
    if (e.target === e.currentTarget) {
      setSelectedId(null);
    }
  };

  const gridWidth = gridCols * (cellSize + GAP) - GAP;
  const gridHeight = gridRows * (cellSize + GAP) - GAP;

  return (
    <div style={{
      width: '100vw',
      height: '100vh',
      backgroundColor: '#000',
      overflow: 'hidden',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      display: 'flex',
      flexDirection: 'column',
    }}>
      {/* Grid container */}
      <div
        onClick={handleBackgroundClick}
        style={{
          flex: 1,
          position: 'relative',
          width: gridWidth,
          height: gridHeight,
          margin: '10px auto',
        }}
      >
        {/* Grid cells */}
        {Array.from({ length: gridRows }).map((_, y) =>
          Array.from({ length: gridCols }).map((_, x) => (
            <GridCell
              key={`${x}-${y}`}
              x={x}
              y={y}
              cellSize={cellSize}
            />
          ))
        )}

        {/* Components */}
        {components.map(comp => (
          <GridComponent
            key={comp.id}
            component={comp}
            onMove={moveComponent}
            onResize={resizeComponent}
            onRemove={removeComponent}
            onUpdateSymbol={updateSymbol}
            isSelected={selectedId === comp.id}
            onSelect={setSelectedId}
            gridCols={gridCols}
            gridRows={gridRows}
            cellSize={cellSize}
          />
        ))}
      </div>

      {/* Palette */}
      <ComponentPalette onAdd={addComponent} />

      {/* Status bar */}
      <div style={{
        padding: '6px 16px',
        backgroundColor: '#0a0a0a',
        borderTop: '1px solid rgba(255,255,255,0.08)',
        color: 'rgba(255,255,255,0.4)',
        fontSize: 10,
        display: 'flex',
        justifyContent: 'space-between',
      }}>
        <span>Components: {components.length}</span>
        <span>Grid: {gridCols}×{gridRows}</span>
        <span>
          {selectedId !== null 
            ? `Selected: ${components.find(c => c.id === selectedId)?.symbol || COMPONENT_TYPES[components.find(c => c.id === selectedId)?.type]?.name || 'None'}` 
            : 'Click to select'}
        </span>
      </div>
    </div>
  );
}
