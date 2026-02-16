import React, { useState, useCallback, useRef } from 'react';

// Grid configuration
const GRID_COLS = 12;
const GRID_ROWS = 8;
const CELL_SIZE = 80;
const GAP = 4;

// Component types that can be added
const COMPONENT_TYPES = {
  PRICE_LADDER: { name: 'Price Ladder', defaultW: 2, defaultH: 4, color: '#1a1a2e' },
  CHART: { name: 'Chart', defaultW: 4, defaultH: 3, color: '#16213e' },
  POSITIONS: { name: 'Positions', defaultW: 3, defaultH: 2, color: '#1a1a2e' },
  ORDER_TICKET: { name: 'Order Ticket', defaultW: 2, defaultH: 2, color: '#0f3460' },
  WATCHLIST: { name: 'Watchlist', defaultW: 2, defaultH: 3, color: '#1a1a2e' },
  NEWS: { name: 'News', defaultW: 3, defaultH: 2, color: '#16213e' },
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
const findAvailablePosition = (components, w, h) => {
  for (let y = 0; y <= GRID_ROWS - h; y++) {
    for (let x = 0; x <= GRID_COLS - w; x++) {
      const testComp = { x, y, w, h };
      if (!checkCollision(components, testComp)) {
        return { x, y };
      }
    }
  }
  return null;
};

// Grid cell component
const GridCell = ({ x, y, onDrop, isHighlighted }) => (
  <div
    style={{
      position: 'absolute',
      left: x * (CELL_SIZE + GAP),
      top: y * (CELL_SIZE + GAP),
      width: CELL_SIZE,
      height: CELL_SIZE,
      backgroundColor: isHighlighted ? 'rgba(74, 144, 226, 0.3)' : 'rgba(255,255,255,0.03)',
      border: '1px solid rgba(255,255,255,0.05)',
      borderRadius: 2,
    }}
    onDragOver={(e) => e.preventDefault()}
    onDrop={() => onDrop(x, y)}
  />
);

// Draggable/Resizable Component
const GridComponent = ({ 
  component, 
  onMove, 
  onResize, 
  onRemove,
  isSelected,
  onSelect 
}) => {
  const [isDragging, setIsDragging] = useState(false);
  const [isResizing, setIsResizing] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const compRef = useRef(null);

  const style = {
    position: 'absolute',
    left: component.x * (CELL_SIZE + GAP),
    top: component.y * (CELL_SIZE + GAP),
    width: component.w * CELL_SIZE + (component.w - 1) * GAP,
    height: component.h * CELL_SIZE + (component.h - 1) * GAP,
    backgroundColor: COMPONENT_TYPES[component.type].color,
    border: isSelected ? '2px solid #4a90e2' : '1px solid rgba(255,255,255,0.1)',
    borderRadius: 4,
    cursor: isDragging ? 'grabbing' : 'grab',
    userSelect: 'none',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    boxShadow: isSelected ? '0 0 20px rgba(74, 144, 226, 0.3)' : '0 4px 12px rgba(0,0,0,0.3)',
    transition: isDragging || isResizing ? 'none' : 'all 0.2s ease',
  };

  const handleMouseDown = (e) => {
    if (e.target.classList.contains('resize-handle')) return;
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
    
    const newX = Math.round((e.clientX - gridRect.left - dragOffset.x) / (CELL_SIZE + GAP));
    const newY = Math.round((e.clientY - gridRect.top - dragOffset.y) / (CELL_SIZE + GAP));
    
    const clampedX = Math.max(0, Math.min(GRID_COLS - component.w, newX));
    const clampedY = Math.max(0, Math.min(GRID_ROWS - component.h, newY));
    
    if (clampedX !== component.x || clampedY !== component.y) {
      onMove(component.id, clampedX, clampedY);
    }
  }, [isDragging, dragOffset, component, onMove]);

  const handleMouseUp = () => {
    setIsDragging(false);
    setIsResizing(false);
  };

  React.useEffect(() => {
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
      
      const newW = Math.max(1, Math.min(GRID_COLS - component.x, startW + Math.round(deltaX / (CELL_SIZE + GAP))));
      const newH = Math.max(1, Math.min(GRID_ROWS - component.y, startH + Math.round(deltaY / (CELL_SIZE + GAP))));
      
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

  return (
    <div ref={compRef} style={style} onMouseDown={handleMouseDown}>
      {/* Header */}
      <div style={{
        padding: '8px 12px',
        borderBottom: '1px solid rgba(255,255,255,0.1)',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: 'rgba(0,0,0,0.2)',
      }}>
        <span style={{ color: '#fff', fontSize: 12, fontWeight: 600 }}>
          {COMPONENT_TYPES[component.type].name}
        </span>
        <button
          onClick={(e) => { e.stopPropagation(); onRemove(component.id); }}
          style={{
            background: 'none',
            border: 'none',
            color: 'rgba(255,255,255,0.5)',
            cursor: 'pointer',
            fontSize: 16,
            padding: '0 4px',
          }}
        >
          ×
        </button>
      </div>
      
      {/* Content placeholder */}
      <div style={{ 
        flex: 1, 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center',
        color: 'rgba(255,255,255,0.3)',
        fontSize: 11,
      }}>
        {component.w}×{component.h}
      </div>
      
      {/* Resize handle */}
      <div
        className="resize-handle"
        onMouseDown={handleResizeMouseDown}
        style={{
          position: 'absolute',
          bottom: 0,
          right: 0,
          width: 16,
          height: 16,
          cursor: 'se-resize',
          background: 'linear-gradient(135deg, transparent 50%, rgba(255,255,255,0.2) 50%)',
        }}
      />
    </div>
  );
};

// Component palette
const ComponentPalette = ({ onAdd }) => (
  <div style={{
    position: 'fixed',
    top: 16,
    right: 16,
    backgroundColor: '#0d0d0d',
    border: '1px solid rgba(255,255,255,0.1)',
    borderRadius: 8,
    padding: 12,
    zIndex: 1000,
  }}>
    <div style={{ color: '#fff', fontSize: 11, marginBottom: 8, fontWeight: 600 }}>
      ADD COMPONENT
    </div>
    {Object.entries(COMPONENT_TYPES).map(([type, config]) => (
      <button
        key={type}
        onClick={() => onAdd(type)}
        style={{
          display: 'block',
          width: '100%',
          padding: '8px 12px',
          marginBottom: 4,
          backgroundColor: config.color,
          border: '1px solid rgba(255,255,255,0.1)',
          borderRadius: 4,
          color: '#fff',
          fontSize: 11,
          cursor: 'pointer',
          textAlign: 'left',
        }}
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

  const addComponent = (type) => {
    const config = COMPONENT_TYPES[type];
    const position = findAvailablePosition(components, config.defaultW, config.defaultH);
    
    if (!position) {
      alert('No space available for this component');
      return;
    }

    const newComponent = {
      id: idCounter,
      type,
      x: position.x,
      y: position.y,
      w: config.defaultW,
      h: config.defaultH,
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

  const removeComponent = (id) => {
    setComponents(components.filter(comp => comp.id !== id));
    if (selectedId === id) setSelectedId(null);
  };

  const handleBackgroundClick = (e) => {
    if (e.target === e.currentTarget) {
      setSelectedId(null);
    }
  };

  return (
    <div style={{
      width: '100vw',
      height: '100vh',
      backgroundColor: '#000',
      overflow: 'hidden',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    }}>
      {/* Grid container */}
      <div
        onClick={handleBackgroundClick}
        style={{
          position: 'relative',
          width: GRID_COLS * (CELL_SIZE + GAP) - GAP,
          height: GRID_ROWS * (CELL_SIZE + GAP) - GAP,
          margin: '20px auto',
        }}
      >
        {/* Grid cells */}
        {Array.from({ length: GRID_ROWS }).map((_, y) =>
          Array.from({ length: GRID_COLS }).map((_, x) => (
            <GridCell
              key={`${x}-${y}`}
              x={x}
              y={y}
              onDrop={() => {}}
              isHighlighted={false}
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
            isSelected={selectedId === comp.id}
            onSelect={setSelectedId}
          />
        ))}
      </div>

      {/* Palette */}
      <ComponentPalette onAdd={addComponent} />

      {/* Status bar */}
      <div style={{
        position: 'fixed',
        bottom: 0,
        left: 0,
        right: 0,
        padding: '8px 16px',
        backgroundColor: '#0d0d0d',
        borderTop: '1px solid rgba(255,255,255,0.1)',
        color: 'rgba(255,255,255,0.5)',
        fontSize: 11,
        display: 'flex',
        justifyContent: 'space-between',
      }}>
        <span>Components: {components.length}</span>
        <span>Grid: {GRID_COLS}×{GRID_ROWS}</span>
        <span>{selectedId !== null ? `Selected: ${COMPONENT_TYPES[components.find(c => c.id === selectedId)?.type]?.name || 'None'}` : 'Click component to select'}</span>
      </div>
    </div>
  );
}
