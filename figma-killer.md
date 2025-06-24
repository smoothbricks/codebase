# Figma Killer: Puck-in-tldraw Integration Specification

## Overview

This document outlines the architecture for creating a tldraw-based design tool that integrates Puck editors as "portal"
shapes, combining tldraw's infinite canvas capabilities with Puck's visual UI building features.

## Core Concept

Users can draw "portals" (frames) on a tldraw canvas, where each portal contains a Puck editor context. This allows for:

- Infinite canvas for organizing multiple UI designs
- Drag-and-drop UI building within each portal
- Shared component libraries across portals
- Seamless switching between drawing and UI design modes

## Architecture Analysis

### tldraw Shape System

Based on analysis of tldraw's shape system:

1. **Custom Shape Creation** (modules/tldraw/apps/examples/src/examples/editable-shape/EditableShapeUtil.tsx:23-97)

   - Extend `BaseBoxShapeUtil` or `ShapeUtil`
   - Define shape type and props using tldraw's validation system
   - Use `HTMLContainer` for rendering HTML content

2. **Interactive Content Pattern**

   - Found in: modules/tldraw/apps/examples/src/examples/interactive-shape/my-interactive-shape-util.tsx:42
   - Documentation: modules/tldraw/apps/docs/content/docs/shapes.mdx:236-240
   - Additional examples:
     - modules/tldraw/apps/examples/src/examples/counter-shape/CounterShape.tsx:42
     - modules/tldraw/apps/examples/src/examples/event-blocker/

   ```typescript
   // Enable pointer events on the container
   <HTMLContainer style={{ pointerEvents: 'all' }}>
     <input
       onPointerDown={(e) => e.stopPropagation()}
       onTouchStart={(e) => e.stopPropagation()}
     />
   </HTMLContainer>
   ```

   - Note: Shapes have `pointerEvents: 'none'` by default and require explicit opt-in

3. **Editable Shape Pattern** (modules/tldraw/apps/examples/src/examples/editable-shape/EditableShapeUtil.tsx:45-84)
   - Override `canEdit()` to enable editing
   - Check editing state: `this.editor.getEditingShapeId() === shape.id`
   - Toggle `pointerEvents` based on editing state
   - Use `stopEventPropagation` to prevent shape selection

### Puck Composition API

Based on analysis of Puck's architecture:

1. **Preview Component** (modules/puck/packages/core/components/Puck/components/Preview/index.tsx:67-176)

   - Standalone drag-and-drop preview component
   - Requires Puck context/store to function
   - Supports iframe mode (can be disabled)
   - Handles pointer event bubbling for drag operations

2. **Composition Pattern** (modules/puck/apps/demo/app/custom-ui/[...puckPath]/client.tsx:232-233)

   ```jsx
   <Puck config={config} data={data} iframe={{ enabled: false }}>
     <Puck.Preview />
   </Puck>
   ```

3. **State Management** (modules/puck/packages/core/components/Puck/index.tsx)
   - Uses Zustand store via `useAppStore`
   - Requires wrapping in Puck provider
   - Each instance needs isolated context

## Portal Duplication

tldraw provides built-in shape duplication that works automatically with custom shapes:

- **Keyboard shortcut**: Ctrl/Cmd+D duplicates selected shapes
- **UI**: Right-click menu → "Duplicate"
- **Alt/Option + drag**: Hold Alt/Option while dragging for positioned duplication
- **Automatic support**: Custom shapes using `ShapeUtil` get duplication for free

When a PuckPortal is duplicated, the shape's props (including `puckData` and `config`) are automatically cloned,
creating a new portal with the same content.

## Technical Implementation

### 1. PuckPortal Shape Definition

```typescript
type DevicePreset = {
  id: string;
  name: string;
  width: number;
  height: number;
  category: 'ios' | 'android' | 'web' | 'custom';
  hasNotch?: boolean;
  statusBarHeight?: number;
  navBarHeight?: number;
};

type SizeLockMode = 'unlocked' | 'locked' | 'horizontal-locked';

type PuckPortalShape = TLBaseShape<
  'puck-portal',
  {
    w: number;
    h: number;
    puckData: Data; // Puck's data structure (content only)
    portalName: string; // User-friendly name
    devicePresetId?: string; // Optional device preset
    sizeLockMode: SizeLockMode; // Size locking state
    showDeviceChrome: boolean; // Whether to show device UI chrome
  }
>;

// Shared config used by all portals
const sharedPuckConfig: Config = {
  components: {
    // All available components defined here once
  },
};

// Common device presets
const DEVICE_PRESETS: DevicePreset[] = [
  // iOS Devices
  {
    id: 'iphone-15-pro',
    name: 'iPhone 15 Pro',
    width: 393,
    height: 852,
    category: 'ios',
    hasNotch: true,
    statusBarHeight: 59,
  },
  { id: 'iphone-se', name: 'iPhone SE', width: 375, height: 667, category: 'ios', statusBarHeight: 20 },
  { id: 'ipad-pro-11', name: 'iPad Pro 11"', width: 834, height: 1194, category: 'ios', statusBarHeight: 24 },

  // Android Devices
  {
    id: 'pixel-7',
    name: 'Pixel 7',
    width: 412,
    height: 915,
    category: 'android',
    statusBarHeight: 24,
    navBarHeight: 48,
  },
  {
    id: 'samsung-s23',
    name: 'Samsung S23',
    width: 360,
    height: 780,
    category: 'android',
    statusBarHeight: 24,
    navBarHeight: 48,
  },

  // Web Breakpoints
  { id: 'web-mobile', name: 'Mobile', width: 375, height: 667, category: 'web' },
  { id: 'web-tablet', name: 'Tablet', width: 768, height: 1024, category: 'web' },
  { id: 'web-laptop', name: 'Laptop', width: 1366, height: 768, category: 'web' },
  { id: 'web-desktop', name: 'Desktop', width: 1920, height: 1080, category: 'web' },
];
```

### 2. Shape Util Implementation

The PuckPortalShapeUtil extends tldraw's BaseBoxShapeUtil to create a shape that can host a Puck editor instance. This
is the core integration point where we bridge tldraw's shape system with Puck's visual editor.

**Why BaseBoxShapeUtil**: We extend BaseBoxShapeUtil
(modules/tldraw/packages/editor/src/lib/editor/shapes/shared/BaseBoxShapeUtil.tsx) because it provides built-in support
for rectangular shapes with resize handles, which is perfect for our portal frames. The box shape type gives us
width/height props and standard resize behavior that we can customize.

**Selection-Based Editing**: The portal shows Puck's interactive preview only when selected, using the lightweight
Render component otherwise. This approach minimizes performance impact when multiple portals exist on the canvas. The
selection state is determined using `this.editor.getOnlySelectedShape()` (reference:
modules/tldraw/packages/editor/src/lib/editor/Editor.ts).

```typescript
class PuckPortalShapeUtil extends BaseBoxShapeUtil<PuckPortalShape> {
  static override type = 'puck-portal' as const

  override canEdit() {
    return true // Enable double-click to edit
  }

  // Override resize behavior based on lock mode
  override canResize(shape: PuckPortalShape) {
    return shape.props.sizeLockMode !== 'locked'
  }

  override onResize(shape: PuckPortalShape, info: TLResizeInfo) {
    const { sizeLockMode, devicePresetId } = shape.props

    if (sizeLockMode === 'horizontal-locked') {
      // Allow only height changes
      return {
        ...shape,
        props: {
          ...shape.props,
          h: info.bounds.height,
          // Clear device preset when manually resizing
          devicePresetId: undefined
        }
      }
    }

    // Unlocked - full resize
    return {
      ...shape,
      props: {
        ...shape.props,
        w: info.bounds.width,
        h: info.bounds.height,
        devicePresetId: undefined // Clear preset on manual resize
      }
    }
  }

  component(shape: PuckPortalShape) {
    // Note: this.editor is tldraw's Editor instance (not Puck's editor)
    // Reference: modules/tldraw/packages/editor/src/lib/editor/shapes/shared/ShapeUtil.ts
    const isSelected = this.editor.getOnlySelectedShape()?.id === shape.id
    const isEditing = this.editor.getEditingShapeId() === shape.id
    const device = shape.props.devicePresetId
      ? DEVICE_PRESETS.find(d => d.id === shape.props.devicePresetId)
      : null

    return (
      <div style={{ position: 'relative', width: shape.props.w, height: shape.props.h }}>
        {/* Device Label */}
        {device && (
          <div style={{
            position: 'absolute',
            top: -30,
            left: 0,
            fontSize: 12,
            color: '#666',
            background: 'white',
            padding: '2px 8px',
            borderRadius: 4,
            border: '1px solid #ddd',
            whiteSpace: 'nowrap'
          }}>
            {device.name} • {device.width}×{device.height}
            {shape.props.sizeLockMode !== 'unlocked' && ' 🔒'}
          </div>
        )}

        {/* Device Chrome */}
        {device && shape.props.showDeviceChrome && (
          <DeviceChrome device={device}>
            <PuckContent
              shape={shape}
              isEditing={isEditing}
              isSelected={isSelected}
            />
          </DeviceChrome>
        )}

        {/* No Chrome */}
        {(!device || !shape.props.showDeviceChrome) && (
          <HTMLContainer
            style={{
              pointerEvents: isEditing ? 'all' : 'none',
              width: '100%',
              height: '100%',
              border: '1px solid #ccc',
              overflow: 'hidden'
            }}
            onPointerDown={isEditing ? stopEventPropagation : undefined}
          >
            <PuckContent
              shape={shape}
              isEditing={isEditing}
              isSelected={isSelected}
            />
          </HTMLContainer>
        )}
      </div>
    )
  }
}

// Separate component for Puck content
function PuckContent({ shape, isEditing, isSelected }) {
  return (isEditing || isSelected) ? (
    <Puck
      config={sharedPuckConfig}
      data={shape.props.puckData}
      iframe={{ enabled: false }}
      onChange={(newData) => {
        this.editor.updateShape({
          id: shape.id,
          props: { puckData: newData }
        })
      }}
    >
      <Puck.Preview />
    </Puck>
  ) : (
    <Render config={sharedPuckConfig} data={shape.props.puckData} />
  )
}
```

**Event Isolation**: Since both tldraw and Puck handle drag events, we must prevent event bubbling when interacting with
Puck content. This is achieved using tldraw's `stopEventPropagation` utility
(modules/tldraw/packages/editor/src/lib/utils/dom.ts) and controlling `pointerEvents` based on editing state.

**Container-Level Event Handling**: You can handle all event propagation at the container level, which is simpler than
adding handlers to every interactive element:

```typescript
<HTMLContainer
  onPointerDown={isEditing ? stopEventPropagation : undefined}
  onPointerMove={isEditing ? stopEventPropagation : undefined}
  onPointerUp={isEditing ? stopEventPropagation : undefined}
  style={{
    pointerEvents: isEditing ? 'all' : 'none',
    // ... other styles
  }}
>
  {/* All Puck content - no need for individual stopPropagation */}
  <Puck config={sharedPuckConfig} data={shape.props.puckData}>
    <Puck.Preview />
  </Puck>
</HTMLContainer>
```

This pattern comes from tldraw's editable shape example
(modules/tldraw/apps/examples/src/examples/editable-shape/EditableShapeUtil.tsx:53) where the container stops all
propagation when editing, allowing all child elements to be interactive without individual handlers.

**Resize Control**: The shape's resize behavior adapts to the lock mode:

- When locked, `canResize()` returns false to hide resize handles entirely
- When horizontal-locked, only height changes are allowed (useful for scrollable mobile views)
- Manual resizing clears the device preset to indicate custom dimensions

**State Synchronization**: Puck's onChange callback updates the tldraw shape's props, ensuring all changes are persisted
and work with tldraw's undo/redo system. This leverages tldraw's built-in state management where shape props are
automatically tracked for history.

### 3. Device Chrome Rendering

When a device preset is selected and chrome is enabled, the portal renders fake device UI to help designers visualize
their work in context. This follows the common pattern seen in design tools like Figma where mockups include device
frames.

The chrome adapts based on device category:

- **iOS**: Renders status bar with optional notch, matching Apple's design language
- **Android**: Shows Material Design status bar and navigation bar
- **Web**: Displays browser window with traffic lights and address bar

This visual context helps designers:

- Understand safe areas and system UI constraints
- Present designs more professionally
- Test responsive layouts at accurate dimensions
- Account for platform-specific UI elements

```typescript
function DeviceChrome({ device, children }: { device: DevicePreset, children: ReactNode }) {
  switch (device.category) {
    case 'ios':
      return (
        <div style={{
          width: '100%',
          height: '100%',
          background: '#f5f5f7',
          borderRadius: device.hasNotch ? 40 : 20,
          overflow: 'hidden',
          position: 'relative',
          boxShadow: '0 0 0 4px #1a1a1a'
        }}>
          {/* Status Bar */}
          <div style={{
            height: device.statusBarHeight,
            background: 'white',
            borderBottom: '1px solid #e5e5e7',
            display: 'flex',
            alignItems: 'center',
            padding: '0 20px',
            fontSize: 12
          }}>
            <span style={{ marginRight: 'auto' }}>9:41</span>
            <span>🔋 📶 📡</span>
          </div>
          {/* Content */}
          <div style={{
            height: `calc(100% - ${device.statusBarHeight}px)`,
            overflow: 'hidden'
          }}>
            {children}
          </div>
        </div>
      )

    case 'android':
      return (
        <div style={{
          width: '100%',
          height: '100%',
          background: '#000',
          borderRadius: 20,
          overflow: 'hidden',
          position: 'relative',
          boxShadow: '0 0 0 3px #333'
        }}>
          {/* Status Bar */}
          <div style={{
            height: device.statusBarHeight,
            background: '#1a1a1a',
            display: 'flex',
            alignItems: 'center',
            padding: '0 16px',
            fontSize: 11,
            color: 'white'
          }}>
            <span style={{ marginRight: 'auto' }}>12:30</span>
            <span>📶 📡 🔋</span>
          </div>
          {/* Content */}
          <div style={{
            height: `calc(100% - ${device.statusBarHeight}px - ${device.navBarHeight}px)`,
            background: 'white',
            overflow: 'hidden'
          }}>
            {children}
          </div>
          {/* Navigation Bar */}
          <div style={{
            height: device.navBarHeight,
            background: '#1a1a1a',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-around'
          }}>
            <span style={{ color: 'white' }}>◀</span>
            <span style={{ color: 'white' }}>●</span>
            <span style={{ color: 'white' }}>■</span>
          </div>
        </div>
      )

    case 'web':
      return (
        <div style={{
          width: '100%',
          height: '100%',
          background: '#f0f0f0',
          borderRadius: 8,
          overflow: 'hidden',
          boxShadow: '0 2px 10px rgba(0,0,0,0.1)'
        }}>
          {/* Browser Chrome */}
          <div style={{
            height: 40,
            background: '#e0e0e0',
            borderBottom: '1px solid #ccc',
            display: 'flex',
            alignItems: 'center',
            padding: '0 12px',
            gap: 8
          }}>
            <div style={{ display: 'flex', gap: 6 }}>
              <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#ff5f57' }} />
              <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#ffbd2e' }} />
              <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#28ca42' }} />
            </div>
            <div style={{
              flex: 1,
              background: 'white',
              borderRadius: 4,
              padding: '4px 12px',
              fontSize: 12,
              color: '#666'
            }}>
              localhost:3000
            </div>
          </div>
          {/* Content */}
          <div style={{
            height: 'calc(100% - 40px)',
            background: 'white',
            overflow: 'hidden'
          }}>
            {children}
          </div>
        </div>
      )

    default:
      return <>{children}</>
  }
}
```

**Why Device Chrome Matters**: Professional design presentations often require showing UI in context. By rendering
platform-specific chrome, designers can:

- Account for safe areas (iOS notch, Android status bar)
- Show stakeholders exactly how the UI will appear to users
- Test whether important content is visible above the fold
- Ensure touch targets aren't too close to system UI

The implementation uses the device preset's metadata (statusBarHeight, navBarHeight, hasNotch) to accurately render the
chrome. This approach is inspired by tools like Framer and Figma that provide similar device frames.

### 3.5. Zoom-to-Portal Feature

For locked portals, we need a zoom feature that provides an immersive preview experience. This addresses a critical
workflow: designers need to see their work at actual size and interact with it as if it were on the target device.

**Zoom Button Implementation**: When a portal has `sizeLockMode !== 'unlocked'`, show a zoom button in the floating UI
or as part of the portal's selection UI. Using tldraw's camera APIs:

```typescript
// First click: Zoom to 100% scale and center
function zoomToPortal(editor: Editor, portalId: string) {
  const shape = editor.getShape(portalId);
  const bounds = editor.getShapePageBounds(portalId);

  if (bounds) {
    // Calculate zoom to show at 100% scale
    const viewport = editor.getViewportScreenBounds();
    const targetZoom = 1; // 100% scale

    if (bounds.height > viewport.height) {
      // Portal taller than viewport - align to top
      editor.setCamera(
        {
          x: -(bounds.x + bounds.width / 2 - viewport.width / 2),
          y: -bounds.y, // Align to top to show device chrome
          z: targetZoom,
        },
        { animation: { duration: 500 } }
      );
    } else {
      // Center the portal
      editor.zoomToBounds(bounds, {
        targetZoom,
        animation: { duration: 500 },
      });
    }
  }
}
```

**Immersive Mode (Second Click)**: Transform the portal into a fullscreen interactive preview:

```typescript
function enterImmersiveMode(portalId: string) {
  // Create overlay component
  return (
    <div style={{
      position: 'fixed',
      top: 0,
      left: 0,
      width: '100vw',
      height: '100vh',
      background: 'rgba(0, 0, 0, 0.9)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 9999,
      overflow: 'auto'
    }}>
      <div style={{
        position: 'relative',
        width: portal.props.w,
        height: portal.props.h,
        maxWidth: '100vw',
        maxHeight: '100vh',
        overflow: 'auto'
      }}>
        {/* Render portal with full interactivity */}
        <Puck config={sharedPuckConfig} data={portal.props.puckData}>
          <Puck.Preview />
        </Puck>
      </div>
    </div>
  )
}
```

**Exit Triggers**: Multiple ways to exit immersive mode enhance usability:

- ESC key: Use `onEscapeKeyDown` handler
- Click device home button (for mobile chrome)
- Click browser close button (for web chrome)
- Click outside the portal area

**Why This Matters**:

- **Accurate Preview**: Designers see exactly how their UI looks at real size
- **Interactive Testing**: Can scroll and interact as users would
- **Presentation Mode**: Perfect for stakeholder reviews
- **Context Switching**: Smooth transition between design and preview modes

**Tooltip Implementation**: Make features discoverable using tldraw's tooltip patterns. tldraw uses Radix UI throughout
their UI system (they import from `radix-ui` package and wrap components with a `TldrawUi` prefix). For consistency:

```typescript
// Simple approach - native HTML tooltips (what tldraw uses in toolbar)
<button
  title="Zoom to actual size (Z)"
  onClick={handleZoom}
  className="tldraw-ui-button"
>
  🔍
</button>

// Or for richer tooltips, use tldraw's Radix UI pattern
import { Tooltip, TooltipTrigger, TooltipContent } from 'radix-ui'

<Tooltip>
  <TooltipTrigger asChild>
    <button className="tldraw-ui-button">🔍</button>
  </TooltipTrigger>
  <TooltipContent>
    <p>View at 100% scale</p>
    <p>Click again for immersive mode</p>
  </TooltipContent>
</Tooltip>
```

**References**:

- Toolbar implementation: modules/tldraw/packages/tldraw/src/lib/ui/components/Toolbar/DefaultToolbarContent.tsx
- Menu item tooltips: modules/tldraw/packages/tldraw/src/lib/ui/components/menus/TldrawUiMenuItem.tsx (uses `title`
  attribute)
- Radix UI imports: modules/tldraw/packages/tldraw/src/lib/ui/lib/radix-ui.tsx

This zoom feature bridges the gap between design and preview, allowing designers to seamlessly switch contexts without
leaving their creative flow. The implementation leverages tldraw's camera system
(modules/tldraw/packages/editor/src/lib/editor/Editor.ts) and React's portal system for the overlay.

### 4. Floating UI Panels

When a PuckPortal is selected (not editing), show floating panels:

1. **Device Preset Selector**

   ```typescript
   function PortalDeviceSelector({ portal, onUpdate }) {
     return (
       <div className="tldraw-ui-panel">
         {/* Size Lock Controls */}
         <div className="device-lock-controls">
           <button
             className={portal.props.sizeLockMode === 'unlocked' ? 'active' : ''}
             onClick={() => onUpdate({ sizeLockMode: 'unlocked' })}
             title="Freely resizable"
           >
             🔓 Unlocked
           </button>
           <button
             className={portal.props.sizeLockMode === 'horizontal-locked' ? 'active' : ''}
             onClick={() => onUpdate({ sizeLockMode: 'horizontal-locked' })}
             title="Lock width, allow height changes"
           >
             ↔️ H-Lock
           </button>
           <button
             className={portal.props.sizeLockMode === 'locked' ? 'active' : ''}
             onClick={() => onUpdate({ sizeLockMode: 'locked' })}
             title="Fully locked"
           >
             🔒 Locked
           </button>
         </div>

         {/* Device Chrome Toggle */}
         <label>
           <input
             type="checkbox"
             checked={portal.props.showDeviceChrome}
             onChange={(e) => onUpdate({ showDeviceChrome: e.target.checked })}
           />
           Show device chrome
         </label>

         {/* Device Presets */}
         <div className="device-presets">
           <button
             className={!portal.props.devicePresetId ? 'active' : ''}
             onClick={() => onUpdate({
               devicePresetId: undefined,
               w: portal.props.w,
               h: portal.props.h
             })}
           >
             Custom Size
           </button>

           {Object.entries(groupBy(DEVICE_PRESETS, 'category')).map(([category, devices]) => (
             <div key={category}>
               <h4>{category.toUpperCase()}</h4>
               {devices.map(device => (
                 <button
                   key={device.id}
                   className={portal.props.devicePresetId === device.id ? 'active' : ''}
                   onClick={() => onUpdate({
                     devicePresetId: device.id,
                     w: device.width,
                     h: device.height,
                     sizeLockMode: 'locked' // Auto-lock when selecting preset
                   })}
                 >
                   {device.name}
                   <span className="device-dims">{device.width}×{device.height}</span>
                 </button>
               ))}
             </div>
           ))}
         </div>
       </div>
     )
   }
   ```

2. **Component Palette**

   - Use tldraw's UI system for consistent styling
   - Position using `editor.getSelectionRotatedScreenBounds()` for selection-relative placement
   - Use `InFrontOfTheCanvas` component slot for floating UI
   - Implementation reference: modules/tldraw/packages/tldraw/src/lib/ui/components/
   - Show Puck.Components when portal selected

3. **Zoom and Presentation Controls**

   - Add zoom button to portal's contextual toolbar
   - Use `track()` from tldraw to make UI reactive to shape state
   - Implement using tldraw's camera APIs: `zoomToBounds()`, `setCamera()`
   - Use React portals for immersive mode overlay
   - Add tooltips using native `title` attribute or Radix UI Tooltip

4. **Drag Handling**
   - Prevent canvas pan during component drag
   - Use `stopPropagation` on drag events
   - Reference: modules/puck/packages/core/components/DragDropContext/index.tsx

### 4. Critical Integration Points

#### Event Isolation

- **Problem**: Both tldraw and Puck use pointer/drag events
- **Solution**:
  ```typescript
  // In PuckPortal component
  onPointerDown={isEditing ? stopEventPropagation : undefined}
  onPointerMove={isEditing ? stopEventPropagation : undefined}
  ```
- **Reference**: stopEventPropagation utility from modules/tldraw/packages/editor/src/lib/utils/dom.ts

#### Single Active Portal

- **Design Decision**: Only the selected portal is editable/interactive
- **Benefits**:
  - Simplifies event handling
  - Better performance (only one Puck instance active)
  - Clear user mental model
  - **Eliminates Puck global state conflicts** - only one Puck instance exists at a time
- **Implementation**: Show Puck.Preview only for selected shape, Render for others

#### Shared Config, Per-Portal Data

- **Architecture**: Single shared Puck config with all component definitions
- **Per-Portal**: Each portal only stores its `data` (content structure)
- **Benefits**:
  - Consistent component library across all portals
  - Smaller shape data (no config duplication)
  - Easy to update components globally
- **Implementation**:
  ```typescript
  type PuckPortalShape = TLBaseShape<
    'puck-portal',
    {
      w: number;
      h: number;
      puckData: Data; // Only data, not config
      portalName: string;
    }
  >;
  ```

#### Component Palette Integration

- **When Portal Selected**: Show floating palette
- **Drag Source**: modules/puck/packages/core/components/Drawer/index.tsx
- **Drop Target**: Only the selected portal's Puck.Preview component
- **Canvas Lock**: Disable tldraw pan during component drag
- **No Cross-Portal Drag**: Components can only be dropped into the currently selected portal

## Implementation Phases

### Phase 1: Basic Portal Shape

- [ ] Create PuckPortalShapeUtil.tsx
- [ ] Basic shape with Puck.Preview/Render toggle
- [ ] Handle view/edit mode based on selection
- [ ] Test with static Puck config
- [ ] Ensure proper event isolation (stopPropagation)

### Phase 2: Core Drag-and-Drop Integration

- [ ] Show floating component palette when portal selected
- [ ] Implement component drag from palette to portal
- [ ] Prevent tldraw canvas movement during drag
- [ ] Ensure drops only work on selected portal
- [ ] Test Puck's internal drag-and-drop within portal

### Phase 3: Selection-Based UI & Fields

- [ ] Refine portal selection detection
- [ ] Show Puck.Fields panel for components within portal
- [ ] Proper layout for floating panels
- [ ] Keyboard shortcuts for common actions

### Phase 4: State Management

- [ ] Persist portal data in shape props
- [ ] Ensure undo/redo works with Puck changes
- [ ] Portal duplication preserves content
- [ ] Import/export portal contents

### Phase 5: Device Presets & Responsive Features

- [ ] Add device preset data structure
- [ ] Implement size lock modes (unlocked/locked/horizontal-locked)
- [ ] Create device preset selector UI
- [ ] Show device dimensions label
- [ ] Add DeviceChrome component for iOS/Android/Web
- [ ] Implement zoom-to-portal for locked sizes
- [ ] Add immersive preview mode with ESC to exit
- [ ] Add tooltips to UI buttons for discoverability

### Phase 6: Polish & Advanced Features

- [ ] Performance optimization (lazy loading)
- [ ] More device presets
- [ ] Custom breakpoint creation
- [ ] Collaborative editing support
- [ ] Export workflows (design to code)
- [ ] Multi-portal workflows

## API Clarification: tldraw Editor vs Puck Editor

### tldraw's Editor API

- **What it is**: The main controller class for tldraw applications
- **Access in ShapeUtil**: `this.editor` property
- **Reference**: modules/tldraw/packages/editor/src/lib/editor/Editor.ts
- **Common methods**:
  - `this.editor.getEditingShapeId()` - Get currently editing shape
  - `this.editor.getOnlySelectedShape()` - Get single selected shape
  - `this.editor.updateShape()` - Update shape properties
  - `this.editor.getZoomLevel()` - Get current zoom
- **Constructor**: ShapeUtil classes receive Editor instance automatically

### Puck's Editor Concept

- **What it is**: A visual page builder/content editor component
- **Not an API class**: It's the overall editing experience
- **Reference**: modules/puck/packages/core/components/Puck/index.tsx
- **Usage**: `<Puck>` component that wraps the editing interface

These are completely separate concepts that happen to share the "editor" name.

## Key Source Files Reference

### tldraw

- Shape system: modules/tldraw/packages/editor/src/lib/editor/shapes/
- Custom shapes: modules/tldraw/apps/examples/src/examples/
- Event handling: modules/tldraw/packages/editor/src/lib/editor/Editor.ts
- UI components: modules/tldraw/packages/tldraw/src/lib/ui/components/

### Puck

- Preview component: modules/puck/packages/core/components/Puck/components/Preview/index.tsx
- Composition API: modules/puck/packages/core/components/Puck/index.tsx
- Drag-drop: modules/puck/packages/core/components/DragDropContext/index.tsx
- Component drawer: modules/puck/packages/core/components/Drawer/index.tsx
- Store: modules/puck/packages/core/store/index.tsx
- Render component: modules/puck/packages/core/components/Render/index.tsx

## Critical Implementation Details Not Obvious from Docs

### Shape Registration and Tool Creation

When creating custom shapes in tldraw, you must also create a corresponding tool for users to add that shape to the
canvas. This isn't obvious but is essential:

```typescript
// Shape tool class (required for toolbar)
class PuckPortalShapeTool extends BaseBoxShapeTool {
  static override id = 'puck-portal'
  static override initial = 'idle'
  override shapeType = 'puck-portal'
}

// Register both shape and tool
const customShapeUtils = [PuckPortalShapeUtil]
const customTools = [PuckPortalShapeTool]

// Option 1: Use existing frame icon (recommended to start)
<Tldraw
  shapeUtils={customShapeUtils}
  tools={customTools}
  overrides={{
    tools(editor, tools) {
      tools.puckPortal = {
        id: 'puck-portal',
        icon: 'tool-frame', // Reuse frame icon - already represents a container
        label: 'Portal',
        kbd: 'p',
        onSelect: () => editor.setCurrentTool('puck-portal')
      }
      return tools
    }
  }}
/>

// Option 2: Add custom icon
const customAssetUrls: TLUiAssetUrlOverrides = {
  icons: {
    'portal-icon': '/icons/portal.svg', // Custom SVG following tldraw conventions
  },
}

<Tldraw
  shapeUtils={customShapeUtils}
  tools={customTools}
  assetUrls={customAssetUrls}
  overrides={{
    tools(editor, tools) {
      tools.puckPortal = {
        id: 'puck-portal',
        icon: 'portal-icon', // Use custom icon
        label: 'Portal',
        kbd: 'p',
        onSelect: () => editor.setCurrentTool('puck-portal')
      }
      return tools
    }
  }}
/>
```

Reference: modules/tldraw/apps/examples/src/examples/custom-shape/CustomShapeExample.tsx

### Icon System Details

**Available Icons**: tldraw includes ~100 built-in icons (modules/tldraw/packages/tldraw/src/lib/ui/icon-types.ts):

- **Suitable existing icons for portals**:
  - `tool-frame` - Shows corner brackets, already represents a container/frame
  - `geo-rectangle` - Simple rectangle
  - `external-link` - Square with arrow, could represent opening in new view
  - `corners` - Corner markers

**Icon Format Requirements** (if creating custom):

- SVG format, 30x30 viewBox
- 2px stroke width, no fill (stroke only)
- Simple, clear design that works at toolbar size
- Example structure:

```svg
<svg viewBox="0 0 30 30" fill="none" xmlns="http://www.w3.org/2000/svg">
  <path d="M 8 8 L 22 8 L 22 22 L 8 22 Z" stroke="currentColor" stroke-width="2"/>
</svg>
```

**Icon Registration**: Icons are defined in the tool configuration and can be:

1. Built-in icon names (e.g., `'tool-frame'`)
2. Custom icons via `assetUrls` override
3. Referenced in `tools` override when adding to toolbar

References:

- Icon types: modules/tldraw/packages/tldraw/src/lib/ui/icon-types.ts
- Tool registration: modules/tldraw/packages/tldraw/src/lib/ui/hooks/useTools.tsx
- Custom assets example: modules/tldraw/apps/examples/src/examples/custom-ui/CustomUiExample.tsx

### Puck's Internal Architecture Constraints

**Hidden Dependency**: Puck expects to be rendered within a consistent React tree. When switching between portals, you
might encounter:

- Lost drag state between portal switches
- Component palette needing re-initialization
- The `@measured/dnd` library maintaining global state

**Solution**: Ensure Puck is fully unmounted before mounting a new instance:

```typescript
{selectedPortalId && (
  <Puck key={selectedPortalId} // Force remount on portal change
    config={sharedPuckConfig}
    data={portals[selectedPortalId].puckData}
  />
)}
```

### tldraw's Coordinate Systems

tldraw has three coordinate systems that must be understood for proper positioning:

- **Screen space**: Browser viewport coordinates
- **Page space**: Canvas coordinates (affected by pan/zoom)
- **Shape space**: Local to each shape

For floating UI positioning:

```typescript
// Convert shape bounds to screen coordinates for UI positioning
const pageBounds = editor.getShapePageBounds(shape);
const screenBounds = editor.pageToScreen(pageBounds);
```

Reference: modules/tldraw/packages/editor/src/lib/editor/Editor.ts

### Shape Update Patterns

tldraw shapes should be treated as immutable. Always spread existing props:

```typescript
// WRONG - will lose other props
editor.updateShape({
  id: shape.id,
  props: { puckData: newData },
});

// CORRECT - preserves all props
editor.updateShape({
  id: shape.id,
  type: shape.type,
  props: { ...shape.props, puckData: newData },
});
```

### Browser Performance Considerations

**HTMLContainer Rendering**: Each HTMLContainer creates a foreign object in SVG, which can impact performance. Consider:

- Rendering only the selected portal with full Puck
- Using CSS transforms instead of re-rendering for animations
- Implementing viewport culling for off-screen portals

### Event System Gotchas

**Pointer Events**: The `pointerEvents` style must be carefully managed:

- Default shapes have `pointerEvents: 'none'` (via CSS class `tl-html-container`)
- Setting to `'all'` enables interaction but also shape selection
- Can use `stopEventPropagation` at container level OR on individual elements

**Container vs Element-Level Stopping**:

```typescript
// Option 1: Container level (simpler, recommended for Puck portals)
<HTMLContainer
  onPointerDown={isEditing ? stopEventPropagation : undefined}
  style={{ pointerEvents: isEditing ? 'all' : 'none' }}
>
  {/* All children are interactive when editing */}
</HTMLContainer>

// Option 2: Element level (more control)
<HTMLContainer style={{ pointerEvents: 'all' }}>
  <button onPointerDown={stopEventPropagation}>Click</button>
  <div>This div will trigger shape selection</div>
</HTMLContainer>
```

**Touch Events**: Don't forget touch events for tablet support:

```typescript
onPointerDown = { stopEventPropagation };
onTouchStart = { stopEventPropagation };
onTouchEnd = { stopEventPropagation };
```

### Shape Props Validation

tldraw uses a validation system for shape props. You must define validators:

```typescript
import { T } from '@tldraw/editor'

static override props: RecordProps<PuckPortalShape> = {
  w: T.number,
  h: T.number,
  puckData: T.object, // For complex objects
  portalName: T.string,
  devicePresetId: T.optional(T.string),
  sizeLockMode: T.literalEnum('unlocked', 'locked', 'horizontal-locked'),
  showDeviceChrome: T.boolean
}
```

Reference: modules/tldraw/packages/tlschema/src/validation.ts

### Puck's Drag-and-Drop Architecture

Puck uses `@dnd-kit` under the hood, which has its own context. Key implications:

- Drag preview rendering happens in a portal
- Drop zones are identified by data attributes
- The DragDropContext must wrap both source and target

Reference: modules/puck/packages/core/components/DragDropContext/index.tsx

### Default Shape Props

Always provide complete default props to avoid runtime errors:

```typescript
getDefaultProps(): PuckPortalShape['props'] {
  return {
    w: 400,
    h: 600,
    puckData: { content: [], root: {} },
    portalName: 'New Portal',
    devicePresetId: undefined,
    sizeLockMode: 'unlocked',
    showDeviceChrome: false
  }
}
```

## Technical Challenges

### 1. Context Isolation

**Issue**: Puck's global store conflicts with multiple instances **Solution**: Investigate creating store factory or
using React portals

### 2. Event Bubbling

**Issue**: Nested drag systems (tldraw canvas + Puck drag-drop) **Solution**: Strategic use of stopPropagation and
pointer-events

### 3. Performance

**Issue**: Multiple Puck instances could be heavy **Solution**:

- Use Render component when not selected/editing
- Lazy load Puck only when needed
- Consider virtualization for off-screen portals

### 4. Floating UI Positioning

**Issue**: Component palette needs smart positioning **Solution**: Use tldraw's existing UI positioning system

## Example Project Structure

```
modules/puck-tldraw/
├── shapes/
│   ├── PuckPortalShape.ts           # Shape type definition
│   └── PuckPortalShapeUtil.tsx      # Shape implementation
├── components/
│   ├── PuckPortalEditor.tsx         # Wrapper for Puck in portal
│   ├── FloatingComponentPalette.tsx # Draggable component list
│   ├── FloatingFieldsPanel.tsx      # Puck.Fields wrapper
│   └── PuckPreviewWrapper.tsx       # Isolated Puck preview
├── hooks/
│   ├── usePuckPortal.ts            # Portal state management
│   ├── useComponentDrag.ts         # Drag from palette to portal
│   └── usePortalSelection.ts       # Track selected portal
├── utils/
│   ├── puckDataAdapter.ts          # Convert between formats
│   ├── eventHandlers.ts            # Shared event utilities
│   └── portalStore.ts              # Multi-portal state management
└── index.ts                        # Public API
```

## Figma Killer Development Tool

### Overview

Figma Killer is a development tool that integrates into your existing Vite setup, allowing you to design UI with Puck
components on a tldraw infinite canvas. It provides hot-reloading support and works seamlessly with complex build setups
like Tamagui.

### Why a Vite Plugin

Modern React projects often require specific build configurations:

1. **CSS-in-JS compilers**: Tamagui, vanilla-extract, styled-components
2. **Framework integrations**: Next.js, Remix, SvelteKit
3. **Custom transformations**: SVG imports, MDX, worker scripts
4. **Monorepo setups**: Custom path resolutions and aliases

By integrating as a Vite plugin, Figma Killer:

- Uses your existing build pipeline with all its configurations
- Benefits from your project's optimizations and transformations
- Maintains a single dev server for both your app and the design tool
- Requires zero additional configuration for complex setups

### Vite Plugin Architecture

The Vite plugin approach is the primary implementation strategy for Figma Killer. It integrates seamlessly with existing
build pipelines, enabling support for:

- **Tamagui**: With its optimizing CSS compiler and React Native Web transforms
- **vanilla-extract**: CSS-in-JS with build-time extraction
- **Emotion/styled-components**: Runtime CSS-in-JS libraries
- **MDX**: For documentation components
- **Path aliases**: Monorepo and custom module resolution

Configuration example showing integration with complex build setups:

```typescript
// vite.config.ts
export default defineConfig({
  plugins: [
    react(),
    tamaguiPlugin({
      /* tamagui options */
    }),
    vanillaExtractPlugin(),
    figmaKiller({
      puckConfig: './src/puck-config.ts',
      canvasesDir: './designs',
      basePath: '/__figma-killer',
    }),
  ],
});
```

### How It Works

**Important Architecture Note**: Figma Killer IS the application served by the Vite plugin. The plugin serves a complete
tldraw-based design tool that loads and hot-reloads the user's Puck configuration. The user doesn't build or modify the
Figma Killer app - they only provide their Puck component configuration.

The plugin uses Vite's `configureServer` hook to add middleware that handles Figma Killer routes:

```typescript
// figma-killer/vite-plugin.ts
export function figmaKiller(options) {
  return {
    name: 'figma-killer',
    configureServer(server) {
      // Add our routes BEFORE user's middleware
      server.middlewares.use((req, res, next) => {
        const url = new URL(req.url, `http://${req.headers.host}`);

        // Handle our app routes
        if (url.pathname.startsWith(options.basePath)) {
          if (req.headers.accept?.includes('text/html')) {
            // Serve our app HTML
            res.setHeader('Content-Type', 'text/html');
            res.end(generateAppHTML(options));
            return;
          }
        }

        // Handle our API routes
        if (url.pathname.startsWith(`${options.basePath}/api/`)) {
          handleCanvasAPI(req, res, options);
          return;
        }

        // Pass through to user's routes
        next();
      });
    },
    // Also handle our app's client code
    resolveId(id) {
      if (id === '/@figma-killer/app') {
        return '\0figma-killer-app'; // Virtual module
      }
    },
    load(id) {
      if (id === '\0figma-killer-app') {
        return generateAppCode(options);
      }
    },
  };
}
```

**Route Isolation**

The plugin avoids conflicts with your existing routes by:

1. **Namespaced prefix**: All routes under `/__figma-killer/*`
2. **Early middleware**: Processes requests before your app's middleware
3. **Virtual modules**: App code served via Vite's virtual module system
4. **Separate API namespace**: Canvas persistence API at `/__figma-killer/api/*`

This follows the pattern used by other dev tools:

- Storybook uses `/__storybook/*`
- Vite HMR uses `/__vite_hmr`
- React DevTools uses `/__react_devtools/*`

### Complete Plugin Implementation

```typescript
// figma-killer/vite-plugin.ts
import type { Plugin, ViteDevServer } from 'vite';
import { promises as fs } from 'fs';
import path from 'path';

interface FigmaKillerOptions {
  puckConfig: string;
  canvasesDir?: string;
  basePath?: string;
}

export function figmaKiller(options: FigmaKillerOptions): Plugin {
  const resolvedOptions = {
    puckConfig: path.resolve(options.puckConfig),
    canvasesDir: path.resolve(options.canvasesDir || './canvases'),
    basePath: options.basePath || '/__figma-killer',
  };

  return {
    name: 'figma-killer',

    configureServer(server: ViteDevServer) {
      // Add middleware before user's middleware
      server.middlewares.use(async (req, res, next) => {
        const url = new URL(req.url!, `http://${req.headers.host}`);

        // Route: Serve Figma Killer app HTML
        if (url.pathname.startsWith(resolvedOptions.basePath)) {
          // API routes
          if (url.pathname.startsWith(`${resolvedOptions.basePath}/api/`)) {
            await handleAPI(req, res, url, resolvedOptions);
            return;
          }

          // App routes - serve HTML
          if (req.headers.accept?.includes('text/html')) {
            res.setHeader('Content-Type', 'text/html');
            res.end(generateAppHTML(url, resolvedOptions));
            return;
          }
        }

        next();
      });
    },

    // Virtual module for the app
    resolveId(id) {
      if (id === '/@figma-killer/app') {
        return '\0figma-killer-app';
      }
    },

    load(id) {
      if (id === '\0figma-killer-app') {
        return generateAppModule(resolvedOptions);
      }
    },
  };
}

// Generate the HTML for the app
function generateAppHTML(url: URL, options: any): string {
  const canvasPath = url.pathname.slice(options.basePath.length + 1) || 'default';

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Figma Killer - Design Tool</title>
  <style>
    body { margin: 0; overflow: hidden; }
    #root { width: 100vw; height: 100vh; }
  </style>
</head>
<body>
  <div id="root"></div>
  <script type="module">
    window.__FIGMA_KILLER__ = {
      basePath: '${options.basePath}',
      canvasPath: '${canvasPath}',
      configPath: '${options.puckConfig}'
    };
  </script>
  <script type="module" src="/@figma-killer/app"></script>
</body>
</html>`;
}

// Generate the virtual module for the app
function generateAppModule(options: any): string {
  return `
import React, { useState, useEffect, useRef, useCallback } from 'react';
import ReactDOM from 'react-dom/client';
import { Tldraw, Editor, getSnapshot, loadSnapshot } from '@tldraw/tldraw';
import '@tldraw/tldraw/tldraw.css';

const { basePath, canvasPath, configPath } = window.__FIGMA_KILLER__;

// Dynamically import the PuckPortal shape utilities
import { puckPortalShapeUtils, PuckPortalShapeTool } from 'figma-killer/shapes';

function App() {
  const [puckConfig, setPuckConfig] = useState(null);
  const [configVersion, setConfigVersion] = useState(0);
  const [lastSaved, setLastSaved] = useState(null);
  const editorRef = useRef(null);
  
  // Load the Puck config dynamically from the user's project
  useEffect(() => {
    import(/* @vite-ignore */ configPath).then(module => {
      const config = module.default || module.config || module.puckConfig;
      setPuckConfig(config);
    }).catch(err => {
      console.error('Failed to load Puck config:', err);
    });
  }, []);
  
  // Set up explicit HMR for the config to ensure updates are detected
  // Note: Plain object exports require explicit HMR handling as React Fast Refresh
  // only automatically handles component exports
  useEffect(() => {
    if (import.meta.hot && configPath) {
      const handleUpdate = async () => {
        const timestamp = Date.now();
        const module = await import(/* @vite-ignore */ \`\${configPath}?t=\${timestamp}\`);
        const config = module.default || module.config || module.puckConfig;
        setPuckConfig(config);
        setConfigVersion(v => v + 1);
        console.log('[Figma Killer] Puck config reloaded');
      };
      
      import.meta.hot.on('vite:beforeUpdate', handleUpdate);
      return () => {
        import.meta.hot.off('vite:beforeUpdate', handleUpdate);
      };
    }
  }, []);
  
  // Auto-save with debouncing
  const saveTimeoutRef = useRef();
  const handlePersist = useCallback(async () => {
    if (!editorRef.current) return;
    
    const snapshot = getSnapshot(editorRef.current.store);
    try {
      const response = await fetch(\`\${basePath}/api/canvas/\${canvasPath}\`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(snapshot)
      });
      
      if (response.ok) {
        setLastSaved(new Date());
      }
    } catch (error) {
      console.error('Failed to save canvas:', error);
    }
  }, []);
  
  // Handle editor mount
  const handleMount = async (editor) => {
    editorRef.current = editor;
    
    // Load existing canvas
    try {
      const response = await fetch(\`\${basePath}/api/canvas/\${canvasPath}\`);
      if (response.ok) {
        const snapshot = await response.json();
        loadSnapshot(editor.store, snapshot);
      }
    } catch (error) {
      console.log('Starting with new canvas');
    }
    
    // Set up auto-save
    editor.store.listen(() => {
      clearTimeout(saveTimeoutRef.current);
      saveTimeoutRef.current = setTimeout(handlePersist, 1000);
    }, { source: 'user', scope: 'document' });
  };
  
  if (!puckConfig) {
    return <div>Loading Puck configuration...</div>;
  }
  
  return (
    <div style={{ position: 'fixed', inset: 0 }}>
      <div style={{
        position: 'absolute',
        top: 8,
        left: 8,
        zIndex: 1000,
        background: 'rgba(0,0,0,0.8)',
        color: 'white',
        padding: '4px 8px',
        borderRadius: 4,
        fontSize: 12
      }}>
        Canvas: /{canvasPath}
      </div>
      
      {lastSaved && (
        <div style={{
          position: 'absolute',
          top: 8,
          right: 8,
          zIndex: 1000,
          background: 'rgba(0,0,0,0.8)',
          color: 'white',
          padding: '4px 8px',
          borderRadius: 4,
          fontSize: 12
        }}>
          Saved {lastSaved.toLocaleTimeString()}
        </div>
      )}
      
      <Tldraw
        onMount={handleMount}
        shapeUtils={[puckPortalShapeUtils(puckConfig, configVersion)]}
        tools={[PuckPortalShapeTool]}
        overrides={{
          tools(editor, tools) {
            tools.puckPortal = {
              id: 'puck-portal',
              icon: 'tool-frame',
              label: 'Portal',
              kbd: 'p',
              onSelect: () => editor.setCurrentTool('puck-portal')
            };
            return tools;
          }
        }}
      />
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
`;
}

// Handle API routes
async function handleAPI(req: any, res: any, url: URL, options: any) {
  const apiPath = url.pathname.slice(`${options.basePath}/api/`.length);

  if (apiPath.startsWith('canvas/')) {
    const canvasName = apiPath.slice('canvas/'.length);
    const filePath = path.join(options.canvasesDir, `${canvasName}.json`);

    if (req.method === 'GET') {
      try {
        const data = await fs.readFile(filePath, 'utf-8');
        res.setHeader('Content-Type', 'application/json');
        res.end(data);
      } catch {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: 'Canvas not found' }));
      }
      return;
    }

    if (req.method === 'POST') {
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', async () => {
        try {
          const body = Buffer.concat(chunks).toString();
          await fs.mkdir(path.dirname(filePath), { recursive: true });
          await fs.writeFile(filePath, body);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ success: true }));
        } catch (error) {
          res.statusCode = 500;
          res.end(JSON.stringify({ error: 'Failed to save canvas' }));
        }
      });
      return;
    }
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: 'Not found' }));
}
```

### Multi-Canvas Routing

The plugin supports multi-level canvas paths that map to a directory structure:

- `/__figma-killer/` → `canvases/default.json`
- `/__figma-killer/projects/mobile` → `canvases/projects/mobile.json`
- `/__figma-killer/team/design-system/buttons` → `canvases/team/design-system/buttons.json`

This allows teams to organize their design work hierarchically.

### Hot Module Reload with Vite

Since Puck configs contain React components and render functions that cannot be serialized as JSON, we use Vite's
built-in HMR system. Vite automatically handles:

- File watching and dependency tracking
- WebSocket connections for hot updates
- Module transformation (JSX → JS)
- React Fast Refresh to preserve component state

**Puck Config Requirements**: As analyzed in modules/puck/apps/demo/config/blocks/Hero/client.tsx, Puck configs contain:

1. **React Components**: The `render` property is a React component reference
2. **Functions**: Fields include async functions like `resolveData`, `fetchList`
3. **JSX Elements**: Custom field renderers return JSX
4. **Module Imports**: Components are imported from other files

Example from the Hero config:

```typescript
export const Hero: ComponentConfig<HeroProps> = {
  fields: {
    url: {
      type: "custom",
      render: ({ value, onChange }) => ( // Returns JSX
        <FieldLabel><AutoField value={value} onChange={onChange} /></FieldLabel>
      ),
    },
  },
  resolveData: async ({ props }) => { /* async function */ },
  render: HeroComponent, // React component reference
};
```

This is why we need Vite - to handle JSX transformation, module resolution, and hot-reloading of React components.

### Expected Puck Config Format

The Puck config file should export a valid Puck configuration. The plugin supports multiple export styles:

```typescript
// puck-config.ts
import type { Config } from '@measured/puck';
import { Button } from './components/Button';
import { Card } from './components/Card';
import { Hero } from './components/Hero';

const config: Config = {
  components: {
    Button: {
      fields: {
        text: { type: 'text' },
        variant: {
          type: 'select',
          options: [
            { label: 'Primary', value: 'primary' },
            { label: 'Secondary', value: 'secondary' },
          ],
        },
      },
      defaultProps: { text: 'Click me', variant: 'primary' },
      render: Button,
    },
    Card: {
      fields: {
        title: { type: 'text' },
        content: { type: 'textarea' },
      },
      render: Card,
    },
    Hero: {
      fields: {
        headline: { type: 'text' },
        image: { type: 'text' },
      },
      render: Hero,
    },
  },
};

// Supported export styles:
export default config;
// export { config };
// export const puckConfig = config;
```

The config file can import components from anywhere in your project - they'll all be hot-reloaded when changed.

### How Vite HMR Works

When you edit a Puck component file:

1. **File Change Detection**: Vite's file watcher detects the change immediately
2. **Dependency Analysis**: Vite traces which modules import the changed file
3. **HMR Boundary**: Our `import.meta.hot.accept()` call creates an HMR boundary
4. **Module Replacement**: Vite sends the updated module over WebSocket
5. **State Preservation**: React Fast Refresh preserves component state where possible
6. **Config Update**: Our handler updates the Puck config and increments the version
7. **Shape Re-render**: The version change triggers PuckPortal shapes to use the new config

The plugin leverages Vite's existing HMR infrastructure, requiring minimal custom code.

### Key Insight: Component Code vs Portal Data

The architecture separates:

- **Puck Config**: Contains React components, render functions, field definitions (hot-reloadable CODE)
- **Portal Data**: The actual content/state within each portal (persisted JSON DATA)

This separation allows:

1. Hot-reloading component code without losing portal content
2. Saving/loading canvas state without serializing React components
3. Updating the component library globally across all portals

### Updated PuckPortalShapeUtil for Hot Reload

```typescript
// PuckPortalShapeUtil.tsx
export function puckPortalShapeUtils(config: Config, configVersion: number) {
  return class PuckPortalShapeUtil extends BaseBoxShapeUtil<PuckPortalShape> {
    static type = 'puck-portal' as const

    component(shape: PuckPortalShape) {
      const editor = this.editor
      const isEditing = editor.getEditingShapeId() === shape.id
      const [isStale, setIsStale] = useState(false)

      // Track config version to handle hot reloads gracefully
      useEffect(() => {
        const shapeConfigVersion = shape.props.configVersion || 0
        if (configVersion > shapeConfigVersion) {
          setIsStale(true)
          // Update shape with new config version
          editor.updateShape({
            id: shape.id,
            type: 'puck-portal',
            props: { ...shape.props, configVersion }
          })
        }
      }, [configVersion])

      // Handle stale config
      if (isStale) {
        return (
          <HTMLContainer>
            <div style={{
              padding: 20,
              textAlign: 'center',
              background: '#fffbe6',
              border: '1px solid #ffe58f'
            }}>
              <p>Components updated - Click to refresh</p>
              <button onClick={() => setIsStale(false)}>
                Refresh Portal
              </button>
            </div>
          </HTMLContainer>
        )
      }

      return (
        <HTMLContainer style={{ pointerEvents: isEditing ? 'all' : 'none' }}>
          <Puck
            config={config} // Always use latest config
            data={shape.props.puckData} // Preserved portal data
            onChange={(data) => {
              editor.updateShape({
                id: shape.id,
                type: 'puck-portal',
                props: { ...shape.props, puckData: data }
              })
            }}
            iframe={{ enabled: false }}
          >
            {isEditing ? <Puck.Preview /> : <Puck.Render />}
          </Puck>
        </HTMLContainer>
      )
    }
  }
}
```

### Data Persistence Strategy

The saved JSON structure combines tldraw's snapshot with our portal data:

```typescript
interface SavedData {
  version: number; // For migrations
  tldrawSnapshot: TLEditorSnapshot;
  metadata: {
    lastModified: string;
    canvasName?: string;
  };
}
```

### Migration Support

Since tldraw has a robust migration system, we'll leverage it for our custom shape:

```typescript
// migrations.ts
import { createMigrationSequence } from '@tldraw/editor';

export const puckPortalMigrations = createMigrationSequence({
  sequenceId: 'com.puck-tldraw.portal',
  retroactive: true,
  sequence: [
    {
      id: 'com.puck-tldraw.portal/v1',
      up: (record: any) => {
        // Migrate old portal data structure if needed
        if (record.type === 'puck-portal' && !record.props.sizeLockMode) {
          record.props.sizeLockMode = 'unlocked';
        }
        return record;
      },
    },
  ],
});
```

### Development Workflow

With the Vite plugin configured:

1. **Start your dev server**: `npm run dev` (or your project's dev command)

2. **Navigate to Figma Killer**: Open `http://localhost:5173/__figma-killer/`

3. **Design workflow**:

   - Create portals using the Portal tool (keyboard shortcut: P)
   - Drag components from Puck into portals
   - Canvas auto-saves to the configured directory

4. **Hot-reload workflow**:
   - Edit your Puck config or component files
   - Changes reflect instantly without losing canvas state
   - Both component code and config structure updates are supported

### Canvas Organization

The file structure mirrors your URL structure:

```
canvases/
├── default.json                    # /__figma-killer/
├── projects/
│   ├── landing-page.json          # /__figma-killer/projects/landing-page
│   └── mobile-app.json            # /__figma-killer/projects/mobile-app
└── team/
    └── design-system/
        ├── buttons.json           # /__figma-killer/team/design-system/buttons
        └── forms.json             # /__figma-killer/team/design-system/forms
```

### Future Enhancements

**Canvas Management API**:

- List available canvases: `GET /__figma-killer/api/canvases`
- Delete canvas: `DELETE /__figma-killer/api/canvas/:name`
- Duplicate canvas: `POST /__figma-killer/api/canvas/:name/duplicate`

**Export/Import Features**:

- Export portal as React component

**Collaboration Features**:

- Real-time collaboration using tldraw's multiplayer support
- Canvas versioning and branching
- Design system synchronization

## Next Steps

1. Build the CLI tool with Vite programmatic API
2. Create the PuckPortalShape with dynamic config loading
3. Implement the multi-canvas routing system
4. Test the development workflow with nested canvas paths
5. Add canvas management UI (list, create, delete)

This architecture provides a foundation for building a powerful design tool that combines the best of both tldraw
(infinite canvas, drawing tools) and Puck (visual UI building, component management), with the added benefit of a smooth
local development experience.
