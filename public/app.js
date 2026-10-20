/* FlowForge designer – Fabric.js grid canvas + inspector + instance monitor. */
(() => {
  'use strict';

  // ───────────── constants & helpers ─────────────
  const GRID = 20, NODE_W = 180, PORT_GAP = 22, PORT_R = 6;
  const FINAL = ['COMPLETED', 'FAILED', 'STOPPED', 'TERMINATED'];
  const $ = (s, r = document) => r.querySelector(s);
  const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const snap = (v) => Math.round(v / GRID) * GRID;
  const clone = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
  const fmtTime = (t) => (t ? new Date(t).toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '–');
  const fmtDur = (ms) => (ms == null ? '–' : ms < 1000 ? `${ms} ms` : ms < 60000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`);
  const chip = (s) => `<span class="chip s-${esc(s)}">${esc(s)}</span>`;

  function h(tag, attrs = {}, ...kids) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
      else if (k === 'html') el.innerHTML = v;
      else if (v === true) el.setAttribute(k, '');
      else if (v !== false && v != null) el.setAttribute(k, v);
    }
    for (const c of kids.flat()) if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(c));
    return el;
  }

  async function api(method, url, body) {
    const r = await fetch(url, { method, headers: body !== undefined ? { 'content-type': 'application/json' } : {}, body: body !== undefined ? JSON.stringify(body) : undefined });
    const text = await r.text();
    let data; try { data = text ? JSON.parse(text) : null; } catch { data = text; }
    if (!r.ok) { const e = new Error(data?.errors ? data.errors.join('\n') : data?.error || `${r.status} ${r.statusText}`); e.status = r.status; e.data = data; throw e; }
    return data;
  }

  let toastTimer;
  function toast(msg, err) {
    const t = $('#toast');
    t.textContent = msg; t.className = 'toast show' + (err ? ' err' : '');
    clearTimeout(toastTimer); toastTimer = setTimeout(() => (t.className = 'toast' + (err ? ' err' : '')), err ? 6000 : 2500);
  }

  // ───────────── state ─────────────
  const S = {
    types: {}, triggerTypes: {}, workflows: [], wf: null, isNew: false, dirty: false,
    sel: null, // { kind: 'node'|'edge', id }
    instances: [], selInst: null, instDetail: null, activities: [], tab: 'activities',
  };
  const views = new Map(); // nodeId -> fabric.Group
  let edgeObjs = [];

  // ───────────── node geometry ─────────────
  function portsFor(n) {
    const t = S.types[n.type]; if (!t) return ['out'];
    if (t.dynamicPorts === 'cases') return [...(n.config?.cases || []).map(String), 'default'];
    if (t.dynamicPorts === 'activity' && n.config?.kind === 'event.receive') return ['out', 'timeout', 'error'];
    return t.ports;
  }
  const hasInput = (n) => n.type !== 'start' && !(n.type === 'signal' && n.config?.mode === 'listen');
  const nodeH = (n) => Math.max(60, snap(portsFor(n).length * PORT_GAP + 18));
  const outY = (n, i) => { const k = portsFor(n).length, hh = nodeH(n); return hh / 2 + (i - (k - 1) / 2) * PORT_GAP; };
  const outPt = (n, port) => { const i = Math.max(0, portsFor(n).indexOf(port)); return { x: n.x + NODE_W, y: n.y + outY(n, i) }; };
  const inPt = (n) => ({ x: n.x, y: n.y + nodeH(n) / 2 });
  const nodeById = (id) => S.wf?.nodes.find((n) => n.id === id);

  function subtitle(n) {
    const t = S.types[n.type];
    const c = n.config || {};
    if (n.type === 'activity') return c.kind || 'activity';
    if (n.type === 'delay') return `delay ${c.duration ?? ''}`;
    if (n.type === 'signal') return `${c.mode || 'wait'} “${c.signalName || ''}”`;
    if (n.type === 'subworkflow') return `→ ${c.workflowId || '?'}`;
    if (n.type === 'decision') return trunc(c.condition || '', 26);
    if (n.type === 'switch') return trunc(c.expression || '', 26);
    if (n.type === 'response') return `HTTP ${c.statusCode ?? 200}`;
    if (n.type === 'timer') return c.mode === 'interval' ? `every ${c.intervalMs} ms ×${c.repeat}` : `at ${trunc(String(c.at || ''), 20)}`;
    return t?.label || n.type;
  }
  const trunc = (s, n) => (s.length > n ? s.slice(0, n - 1) + '…' : s);

  // ───────────── canvas ─────────────
  const stage = $('#stage');
  const canvas = new fabric.Canvas('canvas', { selection: false, preserveObjectStacking: true, targetFindTolerance: 6, fireRightClick: false, stopContextMenu: true });
  fabric.Object.prototype.objectCaching = false;

  function resize() { canvas.setDimensions({ width: stage.clientWidth, height: stage.clientHeight }); syncGrid(); }
  new ResizeObserver(resize).observe(stage);

  function syncGrid() {
    const v = canvas.viewportTransform, z = v[0];
    stage.style.backgroundSize = `${100 * z}px ${100 * z}px, ${100 * z}px ${100 * z}px, ${GRID * z}px ${GRID * z}px, ${GRID * z}px ${GRID * z}px`;
    stage.style.backgroundPosition = `${v[4]}px ${v[5]}px`;
  }

  function buildNodeView(n) {
    const t = S.types[n.type] || { color: '#777', icon: '?', label: n.type };
    const hh = nodeH(n), x = n.x, y = n.y;
    const objs = [];
    const rect = new fabric.Rect({ left: x, top: y, width: NODE_W, height: hh, rx: 9, ry: 9, fill: css('--node-bg'), stroke: css('--line'), strokeWidth: 1.5,
      shadow: new fabric.Shadow({ color: 'rgba(15,20,35,0.12)', blur: 10, offsetY: 3 }) });
    rect.ffRole = 'body';
    objs.push(rect);
    objs.push(new fabric.Rect({ left: x, top: y + 10, width: 4, height: hh - 20, fill: t.color, rx: 2, ry: 2 }));
    objs.push(new fabric.Circle({ left: x + 14, top: y + hh / 2 - 14, radius: 14, fill: t.color }));
    objs.push(new fabric.Text(t.icon, { left: x + 28, top: y + hh / 2, originX: 'center', originY: 'center', fontSize: 14, fill: '#fff', fontFamily: 'IBM Plex Sans, system-ui' }));
    objs.push(new fabric.Text(trunc(n.name || n.id, 18), { left: x + 50, top: y + hh / 2 - 16, fontSize: 13, fontWeight: 600, fill: css('--node-ink'), fontFamily: 'IBM Plex Sans, system-ui' }));
    objs.push(new fabric.Text(trunc(subtitle(n), 22), { left: x + 50, top: y + hh / 2 + 2, fontSize: 11, fill: css('--node-sub'), fontFamily: 'IBM Plex Mono, monospace' }));
    const status = new fabric.Circle({ left: x + NODE_W - 16, top: y + 8, radius: 4, fill: 'transparent' });
    status.ffRole = 'status';
    objs.push(status);
    if (hasInput(n)) {
      const ip = new fabric.Circle({ left: x - PORT_R, top: y + hh / 2 - PORT_R, radius: PORT_R, fill: css('--node-bg'), stroke: css('--edge'), strokeWidth: 2 });
      ip.ffPort = { dir: 'in' };
      objs.push(ip);
    }
    portsFor(n).forEach((p, i) => {
      const py = y + outY(n, i);
      const c = new fabric.Circle({ left: x + NODE_W - PORT_R, top: py - PORT_R, radius: PORT_R, fill: p === 'error' || p === 'timeout' ? '#d9534f' : t.color, stroke: css('--node-bg'), strokeWidth: 2, hoverCursor: 'crosshair' });
      c.ffPort = { dir: 'out', port: p };
      objs.push(c);
      if (p !== 'out') objs.push(new fabric.Text(p, { left: x + NODE_W + 9, top: py - 15, fontSize: 10, fill: css('--muted'), fontFamily: 'IBM Plex Mono, monospace' }));
    });
    const g = new fabric.Group(objs, { hasControls: false, subTargetCheck: true, lockScalingX: true, lockScalingY: true, lockRotation: true, hoverCursor: 'move',
      borderColor: css('--accent'), borderScaleFactor: 2, padding: 4 });
    g.ffNodeId = n.id;
    g.ffOff = { dx: n.x - g.left, dy: n.y - g.top };
    return g;
  }

  function edgePath(e) {
    const a = nodeById(e.from), b = nodeById(e.to);
    if (!a || !b) return null;
    const p1 = outPt(a, e.port || 'out'), p2 = inPt(b);
    const dx = Math.max(50, Math.abs(p2.x - p1.x) / 2) + (p2.x < p1.x ? 60 : 0);
    return { d: `M ${p1.x} ${p1.y} C ${p1.x + dx} ${p1.y} ${p2.x - dx} ${p2.y} ${p2.x - 8} ${p2.y}`, p2 };
  }

  function renderEdges() {
    for (const o of edgeObjs) canvas.remove(o);
    edgeObjs = [];
    (S.wf?.edges || []).forEach((e, idx) => {
      const ep = edgePath(e); if (!ep) return;
      const selected = S.sel?.kind === 'edge' && S.sel.id === idx;
      const col = selected ? css('--accent') : (e.port === 'error' || e.port === 'timeout') ? '#d9534f' : css('--edge');
      const path = new fabric.Path(ep.d, { fill: '', stroke: col, strokeWidth: selected ? 3 : 2, strokeDashArray: e.port === 'error' || e.port === 'timeout' ? [6, 4] : null,
        selectable: true, hasControls: false, hasBorders: false, lockMovementX: true, lockMovementY: true, perPixelTargetFind: true, hoverCursor: 'pointer' });
      path.ffEdge = idx;
      const arrow = new fabric.Triangle({ left: ep.p2.x - 4, top: ep.p2.y, originX: 'center', originY: 'center', width: 10, height: 10, angle: 90, fill: col, evented: false, selectable: false });
      edgeObjs.push(path, arrow);
      canvas.add(path, arrow);
      canvas.sendToBack(arrow); canvas.sendToBack(path);
    });
    canvas.requestRenderAll();
  }

  function renderAll() {
    canvas.clear(); views.clear(); edgeObjs = [];
    if (!S.wf) return;
    for (const n of S.wf.nodes) { const g = buildNodeView(n); views.set(n.id, g); canvas.add(g); }
    renderEdges();
    applyOverlay();
    if (S.sel?.kind === 'node' && views.get(S.sel.id)) canvas.setActiveObject(views.get(S.sel.id));
    canvas.requestRenderAll();
  }

  function rerenderNode(id) {
    const n = nodeById(id); const old = views.get(id);
    if (!n || !old) return renderAll();
    const wasActive = canvas.getActiveObject() === old;
    canvas.remove(old);
    const g = buildNodeView(n); views.set(id, g); canvas.add(g);
    if (wasActive) canvas.setActiveObject(g);
    renderEdges(); applyOverlay();
  }

  // drag nodes (snap to grid)
  canvas.on('object:moving', (o) => {
    const g = o.target; if (!g.ffNodeId) return;
    const n = nodeById(g.ffNodeId);
    n.x = snap(g.left + g.ffOff.dx); n.y = snap(g.top + g.ffOff.dy);
    g.set({ left: n.x - g.ffOff.dx, top: n.y - g.ffOff.dy });
    renderEdges(); markDirty();
  });

  // selection
  canvas.on('selection:created', onSel); canvas.on('selection:updated', onSel);
  canvas.on('selection:cleared', () => { if (S.sel?.kind === 'edge') { S.sel = null; renderEdges(); } S.sel = null; renderInspector(); });
  function onSel(o) {
    const t = o.selected?.[0];
    if (!t) return;
    if (t.ffNodeId) { S.sel = { kind: 'node', id: t.ffNodeId }; renderEdges(); }
    else if (t.ffEdge !== undefined) { S.sel = { kind: 'edge', id: t.ffEdge }; canvas.discardActiveObject(); renderEdges(); }
    renderInspector();
  }

  // connect ports / pan
  let connecting = null, panning = null, spaceDown = false;
  canvas.on('mouse:down', (o) => {
    const e = o.e;
    const port = (o.subTargets || []).find((s) => s.ffPort?.dir === 'out');
    if (port && o.target?.ffNodeId && !e.altKey) {
      const n = nodeById(o.target.ffNodeId);
      const p = outPt(n, port.ffPort.port);
      o.target.lockMovementX = o.target.lockMovementY = true;
      const line = new fabric.Line([p.x, p.y, p.x, p.y], { stroke: css('--accent'), strokeWidth: 2, strokeDashArray: [5, 4], evented: false, selectable: false });
      canvas.add(line);
      connecting = { from: n.id, port: port.ffPort.port, line, group: o.target };
      return;
    }
    if (!o.target || e.altKey || spaceDown) {
      panning = { x: e.clientX, y: e.clientY };
      canvas.setCursor('grabbing');
    }
  });
  canvas.on('mouse:move', (o) => {
    if (connecting) {
      const p = canvas.getPointer(o.e);
      connecting.line.set({ x2: p.x, y2: p.y }); canvas.requestRenderAll();
    } else if (panning) {
      const v = canvas.viewportTransform;
      v[4] += o.e.clientX - panning.x; v[5] += o.e.clientY - panning.y;
      panning = { x: o.e.clientX, y: o.e.clientY };
      canvas.setViewportTransform(v); syncGrid();
    }
  });
  canvas.on('mouse:up', (o) => {
    if (connecting) {
      const c = connecting; connecting = null;
      canvas.remove(c.line);
      c.group.lockMovementX = c.group.lockMovementY = false;
      const pt = canvas.getPointer(o.e);
      const target = canvas.getObjects().filter((x) => x.ffNodeId && x.ffNodeId !== c.from).reverse().find((x) => {
        const n = nodeById(x.ffNodeId);
        return pt.x >= n.x - 12 && pt.x <= n.x + NODE_W && pt.y >= n.y - 6 && pt.y <= n.y + nodeH(n) + 6;
      });
      if (target) {
        const to = nodeById(target.ffNodeId);
        if (!hasInput(to)) toast(`${to.name || to.id} has no input`, true);
        else if (S.wf.edges.some((e) => e.from === c.from && e.to === to.id && (e.port || 'out') === c.port)) toast('already connected');
        else { S.wf.edges.push({ from: c.from, to: to.id, port: c.port }); markDirty(); }
      }
      renderEdges();
    }
    if (panning) { panning = null; canvas.setCursor('default'); }
  });
  canvas.on('mouse:wheel', (o) => {
    let z = canvas.getZoom() * 0.999 ** o.e.deltaY;
    z = Math.min(2.5, Math.max(0.25, z));
    canvas.zoomToPoint({ x: o.e.offsetX, y: o.e.offsetY }, z);
    o.e.preventDefault(); o.e.stopPropagation(); syncGrid();
  });
  canvas.on('mouse:dblclick', (o) => { if (o.target?.ffNodeId) { const inp = $('#inspector input[data-k="name"]'); inp?.focus(); inp?.select(); } });

  function zoomBy(f) { const z = Math.min(2.5, Math.max(0.25, canvas.getZoom() * f)); canvas.zoomToPoint({ x: canvas.width / 2, y: canvas.height / 2 }, z); syncGrid(); }
  function fit() {
    if (!S.wf?.nodes.length) { canvas.setViewportTransform([1, 0, 0, 1, 40, 40]); syncGrid(); return; }
    const xs = S.wf.nodes.flatMap((n) => [n.x, n.x + NODE_W + 50]), ys = S.wf.nodes.flatMap((n) => [n.y, n.y + nodeH(n)]);
    const minX = Math.min(...xs) - 40, maxX = Math.max(...xs) + 40, minY = Math.min(...ys) - 40, maxY = Math.max(...ys) + 40;
    const z = Math.min(1, Math.max(0.25, Math.min(canvas.width / (maxX - minX), canvas.height / (maxY - minY))));
    canvas.setViewportTransform([z, 0, 0, z, -minX * z + (canvas.width - (maxX - minX) * z) / 2, -minY * z + (canvas.height - (maxY - minY) * z) / 2]);
    syncGrid();
  }

  // keyboard
  document.addEventListener('keydown', (e) => {
    const typing = /INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName);
    if ((e.ctrlKey || e.metaKey) && e.key === 's') { e.preventDefault(); save(); return; }
    if (typing) return;
    if (e.code === 'Space') { spaceDown = true; }
    if ((e.key === 'Delete' || e.key === 'Backspace') && S.sel) { e.preventDefault(); deleteSelection(); }
    if ((e.ctrlKey || e.metaKey) && e.key === 'd' && S.sel?.kind === 'node') { e.preventDefault(); duplicateNode(S.sel.id); }
  });
  document.addEventListener('keyup', (e) => { if (e.code === 'Space') spaceDown = false; });

  function deleteSelection() {
    if (!S.sel) return;
    if (S.sel.kind === 'edge') S.wf.edges.splice(S.sel.id, 1);
    else {
      const id = S.sel.id;
      S.wf.nodes = S.wf.nodes.filter((n) => n.id !== id);
      S.wf.edges = S.wf.edges.filter((e) => e.from !== id && e.to !== id);
    }
    S.sel = null; canvas.discardActiveObject(); markDirty(); renderAll(); renderInspector();
  }

  // ───────────── palette ─────────────
  function renderPalette() {
    const pal = $('#palette'); pal.innerHTML = '';
    const cats = {};
    for (const [type, t] of Object.entries(S.types)) (cats[t.category] = cats[t.category] || []).push([type, t]);
    for (const [cat, items] of Object.entries(cats)) {
      pal.append(h('h4', {}, cat));
      for (const [type, t] of items) {
        pal.append(h('div', { class: 'pal-item', draggable: 'true', title: t.description,
          ondragstart: (e) => e.dataTransfer.setData('text/flowforge-node', type),
          onclick: () => { const c = canvas.getVpCenter(); addNode(type, c.x - NODE_W / 2, c.y - 30); } },
        h('span', { class: 'sw', style: `background:${t.color}` }, t.icon), t.label));
      }
    }
  }
  stage.addEventListener('dragover', (e) => { if ([...e.dataTransfer.types].includes('text/flowforge-node')) e.preventDefault(); });
  stage.addEventListener('drop', (e) => {
    const type = e.dataTransfer.getData('text/flowforge-node'); if (!type) return;
    e.preventDefault();
    const r = stage.getBoundingClientRect();
    const p = fabric.util.invertTransform(canvas.viewportTransform);
    const pt = fabric.util.transformPoint(new fabric.Point(e.clientX - r.left, e.clientY - r.top), p);
    addNode(type, pt.x - NODE_W / 2, pt.y - 30);
  });

  function defaultsFor(type) {
    const out = {};
    for (const [k, f] of Object.entries(S.types[type]?.params || {})) if (f.default !== undefined) out[k] = clone(f.default);
    return out;
  }
  function uniqueId(base) { let i = 1, id = base; while (nodeById(id)) id = `${base}${++i}`; return id; }

  function addNode(type, x, y) {
    if (!S.wf) return;
    if (type === 'start' && S.wf.nodes.some((n) => n.type === 'start')) return toast('only one Start node per workflow', true);
    const id = uniqueId(type === 'activity' ? 'act' : type);
    const n = { id, type, name: S.types[type].label, x: snap(x), y: snap(y), config: defaultsFor(type) };
    S.wf.nodes.push(n);
    markDirty(); renderAll();
    S.sel = { kind: 'node', id }; canvas.setActiveObject(views.get(id)); renderInspector(); canvas.requestRenderAll();
  }
  function duplicateNode(id) {
    const src = nodeById(id); if (!src || src.type === 'start') return;
    const n = clone(src); n.id = uniqueId(src.id.replace(/\d+$/, '')); n.x += GRID * 2; n.y += GRID * 2; n.name = `${src.name} copy`;
    S.wf.nodes.push(n); markDirty(); renderAll();
    S.sel = { kind: 'node', id: n.id }; canvas.setActiveObject(views.get(n.id)); renderInspector();
  }

  // ───────────── inspector ─────────────
  function visible(f, cfg) {
    if (!f.showIf) return true;
    return Object.entries(f.showIf).every(([k, v]) => (Array.isArray(v) ? v.map(String).includes(String(cfg[k])) : String(cfg[k]) === String(v)));
  }

  /** Builds an input for a schema field. onChange(value) gets the parsed value; returns false to signal invalid input. */
  function fieldInput(key, f, value, onChange) {
    const wrapCls = f.type === 'boolean' ? 'field check' : 'field';
    const label = h('label', { for: `f_${key}` }, f.label || key);
    let input; const err = h('div', { class: 'err' });
    const commit = (v) => { err.textContent = ''; onChange(v); };
    switch (f.type) {
      case 'boolean':
        input = h('input', { type: 'checkbox', id: `f_${key}`, onchange: (e) => commit(e.target.checked) });
        input.checked = !!value;
        return h('div', { class: wrapCls }, input, label);
      case 'select':
        input = h('select', { id: `f_${key}`, onchange: (e) => commit(typeof f.options[0] === 'number' ? Number(e.target.value) : e.target.value) },
          f.options.map((o) => h('option', { value: o, selected: String(o) === String(value) }, String(o))));
        break;
      case 'workflow':
        input = h('select', { id: `f_${key}`, onchange: (e) => commit(e.target.value) },
          h('option', { value: '' }, '— choose —'),
          S.workflows.filter((w) => w.id !== S.wf.id).map((w) => h('option', { value: w.id, selected: w.id === value }, `${w.name} (${w.id})`)));
        break;
      case 'number':
        input = h('input', { type: 'number', id: `f_${key}`, value: value ?? '', min: f.min, oninput: (e) => commit(e.target.value === '' ? undefined : Number(e.target.value)) });
        break;
      case 'json': {
        const text = value === undefined ? '' : typeof value === 'string' ? value : JSON.stringify(value, null, 2);
        input = h('textarea', { id: `f_${key}`, rows: Math.min(10, Math.max(2, text.split('\n').length)), spellcheck: 'false',
          oninput: (e) => {
            const s = e.target.value; const tr = s.trim();
            if (tr === '') return commit(undefined);
            if (/^[[{"]|^-?\d|^(true|false|null)$/.test(tr)) {
              try { commit(JSON.parse(tr)); } catch (x) { err.textContent = `JSON: ${x.message}`; }
            } else commit(s); // template string such as {{ vars }}
          } }, text);
        break;
      }
      case 'code':
        input = h('textarea', { id: `f_${key}`, class: 'code', spellcheck: 'false', oninput: (e) => commit(e.target.value),
          onkeydown: (e) => { if (e.key === 'Tab') { e.preventDefault(); const t = e.target; const s = t.selectionStart; t.setRangeText('  ', s, t.selectionEnd, 'end'); commit(t.value); } } }, value ?? '');
        break;
      case 'list':
        input = h('textarea', { id: `f_${key}`, rows: 3, spellcheck: 'false', oninput: (e) => commit(e.target.value.split('\n').map((s) => s.trim()).filter(Boolean)) }, (value || []).join('\n'));
        break;
      default:
        input = h('input', { type: 'text', id: `f_${key}`, value: value ?? '', placeholder: f.placeholder || '', spellcheck: 'false',
          style: f.type === 'expression' || f.type === 'duration' ? 'font-family:var(--mono);font-size:12px' : '', oninput: (e) => commit(e.target.value) });
    }
    return h('div', { class: wrapCls }, label, input, err);
  }

  function renderInspector() {
    const box = $('#inspector'); box.innerHTML = '';
    if (!S.wf) { box.append(h('div', { class: 'muted' }, 'No workflow loaded.')); return; }
    if (S.sel?.kind === 'node') return renderNodeInspector(box, nodeById(S.sel.id));
    if (S.sel?.kind === 'edge') {
      const e = S.wf.edges[S.sel.id]; if (!e) return;
      box.append(h('h3', {}, 'Connection'),
        h('p', { class: 'desc' }, `${nodeById(e.from)?.name} `, h('code', {}, e.port || 'out'), ` → ${nodeById(e.to)?.name}`),
        h('button', { class: 'ghost danger', onclick: deleteSelection }, 'Delete connection'));
      return;
    }
    renderWorkflowInspector(box);
  }

  function renderNodeInspector(box, n) {
    if (!n) return;
    const t = S.types[n.type];
    box.append(
      h('h3', {}, `${t.icon}  ${t.label}`),
      h('p', { class: 'desc' }, t.description),
      fieldInput('name', { type: 'string', label: 'Name' }, n.name, (v) => { n.name = v; markDirty(); rerenderNode(n.id); }),
      h('div', { class: 'kv' }, `id: ${n.id}`),
    );
    box.lastChild.style.marginBottom = '12px';
    box.querySelector('input').dataset.k = 'name';
    const params = h('div');
    const draw = () => {
      params.innerHTML = '';
      for (const [k, f] of Object.entries(t.params)) {
        if (!visible(f, n.config)) continue;
        params.append(fieldInput(k, f, n.config[k], (v) => {
          if (v === undefined) delete n.config[k]; else n.config[k] = v;
          markDirty();
          const structural = ['kind', 'cases', 'mode', 'status', 'source', 'operation'].includes(k);
          if (structural) { pruneEdges(n); draw(); }
          rerenderNode(n.id);
        }));
      }
    };
    draw();
    box.append(params);
    if (n.type !== 'start' && n.type !== 'end') box.append(h('div', { class: 'help', style: 'margin-top:8px' }, 'Templates: ', h('code', {}, '{{ input.x }}'), ' ', h('code', {}, '{{ vars.y }}'), ' ', h('code', {}, '{{ nodes.<id>.z }}'), ' ', h('code', {}, '{{ signals.name }}'), ' ', h('code', {}, '{{ env.WF_KEY }}')));
    box.append(h('div', { class: 'section' },
      h('button', { class: 'ghost small', onclick: () => duplicateNode(n.id), disabled: n.type === 'start' }, 'Duplicate'), ' ',
      h('button', { class: 'ghost small danger', onclick: deleteSelection }, 'Delete node')));
    const last = S.activities.filter((a) => a.nodeId === n.id).pop();
    if (last && S.instDetail?.workflowId === S.wf.id) {
      box.append(h('div', { class: 'section' },
        h('div', { class: 'section-head' }, h('h4', {}, 'Last run in selected instance'), h('span', { html: chip(last.status) })),
        last.error ? h('pre', { class: 'lv-error' }, last.error) : null,
        last.input !== undefined ? h('div', {}, h('div', { class: 'kv' }, 'input'), h('pre', {}, JSON.stringify(last.input, null, 2))) : null,
        last.output !== undefined ? h('div', {}, h('div', { class: 'kv', style: 'margin-top:6px' }, `output → ${last.port || ''}`), h('pre', {}, JSON.stringify(last.output, null, 2))) : null));
    }
  }

  function pruneEdges(n) {
    const ports = portsFor(n);
    const before = S.wf.edges.length;
    S.wf.edges = S.wf.edges.filter((e) => e.from !== n.id || ports.includes(e.port || 'out'));
    if (!hasInput(n)) S.wf.edges = S.wf.edges.filter((e) => e.to !== n.id);
    if (before !== S.wf.edges.length) toast(`${before - S.wf.edges.length} connection(s) removed – port no longer exists`);
  }

  function renderWorkflowInspector(box) {
    const wf = S.wf;
    box.append(h('h3', {}, 'Workflow'), h('p', { class: 'desc' }, `${wf.nodes.length} nodes · ${wf.edges.length} connections${wf.version ? ` · v${wf.version}` : ''}`));
    const idField = fieldInput('id', { type: 'string', label: 'Workflow id' }, wf.id, (v) => { wf.id = v.trim(); markDirty(); });
    if (!S.isNew) idField.querySelector('input').disabled = true;
    box.append(idField,
      fieldInput('name', { type: 'string', label: 'Name' }, wf.name, (v) => { wf.name = v; markDirty(); }),
      fieldInput('description', { type: 'json', label: 'Description' }, wf.description || '', (v) => { wf.description = typeof v === 'string' ? v : JSON.stringify(v); markDirty(); }),
      fieldInput('enabled', { type: 'boolean', label: 'Enabled (triggers armed)' }, wf.enabled !== false, (v) => { wf.enabled = v; markDirty(); }),
      fieldInput('maxSteps', { type: 'number', label: 'Max steps per run (loop guard)' }, wf.settings?.maxSteps ?? 10000, (v) => { wf.settings = { ...(wf.settings || {}), maxSteps: v }; markDirty(); }));

    const sec = h('div', { class: 'section' });
    sec.append(h('div', { class: 'section-head' }, h('h4', {}, 'Triggers'),
      h('button', { class: 'ghost small', onclick: () => { wf.triggers = [...(wf.triggers || []), { type: 'http', method: 'POST', path: `/${wf.id}`, sync: true }]; markDirty(); renderInspector(); } }, '+ Add')));
    (wf.triggers || []).forEach((t, i) => {
      const spec = S.triggerTypes[t.type] || { params: {} };
      const card = h('div', { class: 'trigger-card' });
      card.append(h('div', { class: 'row' },
        h('select', { onchange: (e) => { wf.triggers[i] = { type: e.target.value }; for (const [k, f] of Object.entries(S.triggerTypes[e.target.value].params)) if (f.default !== undefined) wf.triggers[i][k] = clone(f.default); markDirty(); renderInspector(); } },
          Object.entries(S.triggerTypes).map(([k, v]) => h('option', { value: k, selected: k === t.type }, v.label))),
        h('label', { class: 'small', title: 'enabled' }, h('input', { type: 'checkbox', checked: t.enabled !== false, onchange: (e) => { t.enabled = e.target.checked; markDirty(); } })),
        h('button', { class: 'ghost small danger', title: 'remove', onclick: () => { wf.triggers.splice(i, 1); markDirty(); renderInspector(); } }, '✕')));
      for (const [k, f] of Object.entries(spec.params)) card.append(fieldInput(`t${i}_${k}`, { ...f, label: f.label || k }, t[k], (v) => { if (v === undefined) delete t[k]; else t[k] = v; markDirty(); }));
      if (t.type === 'http' && t.path) card.append(h('div', { class: 'help' }, 'Endpoint: ', h('code', {}, `${t.method || 'POST'} ${location.origin}/hooks${t.path.startsWith('/') ? '' : '/'}${t.path}`)));
      sec.append(card);
    });
    if (!(wf.triggers || []).length) sec.append(h('div', { class: 'help' }, 'No triggers – start it with Run or POST /api/workflows/', h('code', {}, wf.id), '/start'));
    box.append(sec);
  }

  // ───────────── workflow CRUD ─────────────
  function markDirty() { S.dirty = true; $('#dirty').hidden = false; }
  function markClean() { S.dirty = false; $('#dirty').hidden = true; }

  async function loadWorkflows(selectId) {
    S.workflows = await api('GET', '/api/workflows');
    const sel = $('#wfSelect'); sel.innerHTML = '';
    for (const w of S.workflows) sel.append(h('option', { value: w.id }, `${w.name}${w.enabled === false ? ' (disabled)' : ''}`));
    if (S.isNew && S.wf) sel.append(h('option', { value: S.wf.id }, `${S.wf.name} (new)`));
    const id = selectId || S.wf?.id || S.workflows[0]?.id;
    if (id) sel.value = id;
  }

  async function openWorkflow(id) {
    if (S.dirty && !confirm('Discard unsaved changes?')) { $('#wfSelect').value = S.wf.id; return; }
    S.wf = await api('GET', `/api/workflows/${encodeURIComponent(id)}`);
    S.wf.edges = S.wf.edges || []; S.wf.triggers = S.wf.triggers || [];
    S.isNew = false; S.sel = null; markClean();
    try { localStorage.setItem('ff.lastWf', id); } catch {}
    renderAll(); fit(); renderInspector();
    S.selInst = null; S.instDetail = null; S.activities = []; renderDetail();
    loadInstances();
  }

  async function save() {
    if (!S.wf) return false;
    try {
      const body = { ...S.wf };
      const saved = S.isNew ? await api('POST', '/api/workflows', body) : await api('PUT', `/api/workflows/${encodeURIComponent(S.wf.id)}`, body);
      S.wf.version = saved.version; S.isNew = false; markClean();
      await loadWorkflows(saved.id);
      toast(`Saved ${saved.id} (v${saved.version})`);
      renderInspector();
      return true;
    } catch (e) { toast(e.message, true); return false; }
  }

  async function validate() {
    const r = await api('POST', '/api/workflows/validate', S.wf);
    toast(r.valid ? '✓ Workflow is valid' : r.errors.join('\n'), !r.valid);
  }

  function newWorkflow() {
    if (S.dirty && !confirm('Discard unsaved changes?')) return;
    dialog('New workflow', [
      { key: 'id', label: 'Workflow id', placeholder: 'invoice-sync', required: true },
      { key: 'name', label: 'Name', placeholder: 'Invoice sync' },
    ], async (v) => {
      if (S.workflows.some((w) => w.id === v.id)) { toast('id already exists', true); return false; }
      S.wf = { id: v.id, name: v.name || v.id, description: '', enabled: true, triggers: [], settings: {}, edges: [],
        nodes: [{ id: 'start', type: 'start', name: 'Start', x: 60, y: 160, config: defaultsFor('start') }] };
      S.isNew = true; S.sel = null; markDirty();
      await loadWorkflows(v.id);
      renderAll(); fit(); renderInspector(); S.instances = []; renderInstances(); S.selInst = null; renderDetail();
    }, 'Create');
  }

  async function deleteWorkflow() {
    if (!S.wf || !confirm(`Delete workflow "${S.wf.id}"? Running instances keep running.`)) return;
    if (!S.isNew) await api('DELETE', `/api/workflows/${encodeURIComponent(S.wf.id)}`);
    S.wf = null; S.isNew = false; markClean();
    await loadWorkflows();
    if (S.workflows[0]) openWorkflow(S.workflows[0].id); else { renderAll(); renderInspector(); }
  }

  async function run() {
    if (!S.wf) return;
    if (S.dirty || S.isNew) { if (!(await save())) return; }
    const sample = guessInput();
    dialog(`Run ${S.wf.name}`, [
      { key: 'instanceId', label: 'Instance id (blank = auto-generate)', placeholder: 'order-1001' },
      { key: 'input', label: 'Input (JSON)', type: 'textarea', value: JSON.stringify(sample, null, 2) },
    ], async (v) => {
      let input; try { input = v.input.trim() ? JSON.parse(v.input) : {}; } catch (e) { toast(`Input JSON: ${e.message}`, true); return false; }
      try { localStorage.setItem(`ff.input.${S.wf.id}`, JSON.stringify(input)); } catch {}
      try {
        const inst = await api('POST', `/api/workflows/${encodeURIComponent(S.wf.id)}/start`, { instanceId: v.instanceId || undefined, input });
        toast(`Started ${inst.instanceId}`);
        $('#drawer').classList.remove('collapsed');
        await loadInstances();
        selectInstance(inst.instanceId);
      } catch (e) { toast(e.message, true); return false; }
    }, 'Start');
  }
  function guessInput() {
    try { const last = JSON.parse(localStorage.getItem(`ff.input.${S.wf.id}`) || 'null'); if (last) return last; } catch {}
    if (S.wf.id === 'order-approval') return { orderId: `ORD-${Math.floor(Math.random() * 9000 + 1000)}`, items: [{ sku: 'A', price: 400, qty: 3 }] };
    return {};
  }

  // ───────────── dialogs ─────────────
  function dialog(title, fields, onOk, okLabel = 'OK') {
    const dlg = $('#dlg'), form = $('#dlgForm');
    form.innerHTML = '';
    form.append(h('h3', {}, title));
    const inputs = {};
    for (const f of fields) {
      const el = f.type === 'textarea' ? h('textarea', { rows: f.rows || 8, spellcheck: 'false' }, f.value || '')
        : f.type === 'select' ? h('select', {}, f.options.map((o) => h('option', { value: o, selected: o === f.value }, o)))
          : h('input', { type: f.type || 'text', value: f.value ?? '', placeholder: f.placeholder || '', required: f.required });
      inputs[f.key] = el;
      form.append(h('div', { class: 'field' }, h('label', {}, f.label), el, f.help ? h('div', { class: 'help' }, f.help) : null));
    }
    form.append(h('div', { class: 'btns' }, h('button', { type: 'button', class: 'ghost', onclick: () => dlg.close() }, 'Cancel'), h('button', { type: 'submit', value: 'ok' }, okLabel)));
    form.onsubmit = async (e) => {
      e.preventDefault();
      const vals = Object.fromEntries(Object.entries(inputs).map(([k, el]) => [k, el.value]));
      if (fields.some((f) => f.required && !vals[f.key].trim())) return;
      const r = await onOk(vals);
      if (r !== false) dlg.close();
    };
    dlg.showModal();
    (Object.values(inputs)[0])?.focus();
  }

  async function settings() {
    const r = await api('GET', '/api/settings/log-retention');
    let theme = 'auto'; try { theme = localStorage.getItem('ff.theme') || 'auto'; } catch {}
    dialog('Settings', [
      { key: 'retention', label: 'Log retention (hours)', type: 'number', value: r.retentionHours, help: 'Older log entries are purged (Mongo TTL index / etcd lease / periodic purge).' },
      { key: 'theme', label: 'Theme', type: 'select', options: ['auto', 'light', 'dark'], value: theme },
    ], async (v) => {
      try {
        const x = await api('PUT', '/api/settings/log-retention', { retentionHours: Number(v.retention) });
        applyTheme(v.theme);
        toast(`Retention ${x.retentionHours}h · purged ${x.purged} entries`);
      } catch (e) { toast(e.message, true); return false; }
    }, 'Save');
  }
  async function showCluster() {
    let c;
    try { c = await api('GET', '/api/cluster'); } catch (e) { return toast(e.message, true); }
    const dlg = $('#dlg'), form = $('#dlgForm');
    form.innerHTML = '';
    const ago = (t) => (t ? `${Math.max(0, Math.round((Date.now() - t) / 1000))}s ago` : '–');
    const mb = (b) => (b ? `${Math.round(b / 1048576)} MB` : '–');
    form.append(
      h('h3', {}, 'Cluster'),
      h('p', { class: 'desc muted' }, `Coordinator: ${c.coordinator} · leader ${c.leader?.nodeId || '—'} (term ${c.term}) · you are on ${c.self.nodeId}${c.self.isLeader ? ' (leader)' : ' (follower)'}`),
      h('div', { class: 'kv' }, 'MASTERS'),
      h('table', { class: 'acts' }, h('tbody', {}, c.masters.map((m) => h('tr', {},
        h('td', {}, m.nodeId), h('td', { html: m.leader ? chip('LEADER') : '<span class="muted small">follower</span>' }), h('td', { class: 'kv' }, m.grpcAddress), h('td', { class: 'kv' }, m.httpUrl), h('td', { class: 'kv' }, ago(m.lastSeen)))))),
      h('div', { class: 'kv', style: 'margin-top:12px' }, `WORKERS · ${c.running ?? '?'} running · ${c.queued ?? '?'} queued${c.orphaned?.length ? ` · ${c.orphaned.length} awaiting re-attach` : ''}`),
      h('table', { class: 'acts' }, h('thead', {}, h('tr', {}, ['Worker', 'State', 'Load', 'Tags', 'RSS', 'Host'].map((x) => h('th', {}, x)))),
        h('tbody', {}, c.workers.map((w) => h('tr', {},
          h('td', {}, w.id), h('td', { html: chip(w.connected ? (w.draining ? 'DRAINING' : 'CONNECTED') : 'OFFLINE') }),
          h('td', { class: 'kv' }, w.connected ? `${w.assigned}/${w.capacity}` : '–'), h('td', { class: 'kv' }, (w.tags || []).join(', ')),
          h('td', { class: 'kv' }, mb(w.rss)), h('td', { class: 'kv' }, w.hostname || w.httpUrl || ''))))),
      c.error ? h('p', { class: 'lv-error small' }, c.error) : '',
      h('div', { class: 'btns' }, h('button', { type: 'button', class: 'ghost', onclick: () => dlg.close() }, 'Close'), h('button', { type: 'button', onclick: () => { dlg.close(); showCluster(); } }, 'Refresh')));
    form.onsubmit = (e) => e.preventDefault();
    dlg.style.width = 'min(760px, 94vw)';
    dlg.showModal();
    dlg.addEventListener('close', () => { dlg.style.width = ''; }, { once: true });
  }

  function applyTheme(t) {
    try { localStorage.setItem('ff.theme', t); } catch {}
    if (t === 'auto') delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
    renderAll();
  }

  // ───────────── instances ─────────────
  async function loadInstances() {
    const q = new URLSearchParams({ limit: 100 });
    if (!$('#allWorkflows').checked && S.wf && !S.isNew) q.set('workflowId', S.wf.id);
    if ($('#statusFilter').value) q.set('status', $('#statusFilter').value);
    if (!$('#allWorkflows').checked && S.isNew) { S.instances = []; return renderInstances(); }
    const r = await api('GET', `/api/instances?${q}`);
    S.instances = r.items; renderInstances();
  }

  function renderInstances() {
    const box = $('#instList'); box.innerHTML = '';
    if (!S.instances.length) { box.append(h('div', { class: 'empty muted', style: 'padding:14px' }, 'No instances yet. Press ▶ Run or call a trigger.')); return; }
    for (const i of S.instances) {
      const dur = i.startedAt ? (i.endedAt || Date.now()) - i.startedAt : null;
      box.append(h('div', { class: `inst-row${i.instanceId === S.selInst ? ' sel' : ''}`, onclick: () => selectInstance(i.instanceId) },
        h('span', { class: 'id', title: i.instanceId }, i.instanceId),
        h('span', { html: chip(i.status) }),
        h('span', { class: 'meta' }, `${$('#allWorkflows').checked ? i.workflowId + ' · ' : ''}${i.trigger?.type || 'api'} · ${fmtTime(i.createdAt)} · ${fmtDur(dur)}${i.currentNodes?.length ? ' · at ' + i.currentNodes.join(', ') : ''}`)));
    }
  }

  async function selectInstance(id) {
    S.selInst = id;
    renderInstances();
    await refreshDetail();
  }

  async function refreshDetail() {
    if (!S.selInst) return renderDetail();
    try {
      const [inst, acts] = await Promise.all([api('GET', `/api/instances/${encodeURIComponent(S.selInst)}`), api('GET', `/api/instances/${encodeURIComponent(S.selInst)}/activities`)]);
      S.instDetail = inst; S.activities = acts;
    } catch (e) { S.instDetail = null; S.activities = []; }
    renderDetail(); applyOverlay();
    if (S.sel?.kind === 'node') renderInspector();
  }

  function renderDetail() {
    const box = $('#instDetail'); box.innerHTML = '';
    const i = S.instDetail;
    if (!S.selInst || !i) { box.append(h('div', { class: 'empty' }, 'Select an instance to see its activities, logs and request data.')); return; }
    const live = !FINAL.includes(i.status);
    const ctl = h('div', { class: 'ctl' });
    const act = (label, fn, cls = 'ghost small') => h('button', { class: cls, onclick: fn }, label);
    if (live) {
      ctl.append(act('⚡ Signal', sendSignal));
      ctl.append(i.status === 'PAUSED' ? act('Resume', () => control('resume')) : act('Pause', () => control('pause')));
      ctl.append(act('Stop', () => control('stop')), act('Terminate', () => { if (confirm('Kill this workflow process now?')) control('terminate'); }, 'ghost small danger'));
    } else {
      ctl.append(act('↻ Retry', async () => { try { const n = await api('POST', `/api/instances/${encodeURIComponent(i.instanceId)}/retry`, {}); toast(`Retry started: ${n.instanceId}`); await loadInstances(); selectInstance(n.instanceId); } catch (e) { toast(e.message, true); } }));
      ctl.append(act('Delete', async () => { await api('DELETE', `/api/instances/${encodeURIComponent(i.instanceId)}`); S.selInst = null; S.instDetail = null; loadInstances(); renderDetail(); applyOverlay(); }, 'ghost small danger'));
    }
    if (i.workflowId !== S.wf?.id) ctl.prepend(act('Open workflow', () => openWorkflow(i.workflowId).then(() => selectInstance(i.instanceId))));
    const dur = i.startedAt ? (i.endedAt || Date.now()) - i.startedAt : null;
    box.append(h('div', { class: 'det-head' },
      h('span', { class: 'id' }, i.instanceId), h('span', { html: chip(i.status) }),
      h('span', { class: 'muted small' }, `${i.workflowId} v${i.workflowVersion} · pid ${i.pid ?? '–'} · ${fmtDur(dur)}${i.parentInstanceId ? ' · parent ' + i.parentInstanceId : ''}${i.retryOf ? ' · retry of ' + i.retryOf : ''}`),
      ctl));
    const tabs = h('div', { class: 'tabs' });
    for (const [k, label] of [['activities', `Activities (${S.activities.length})`], ['logs', 'Logs'], ['request', 'Request'], ['output', 'Output & vars']]) {
      tabs.append(h('button', { class: S.tab === k ? 'on' : '', onclick: () => { S.tab = k; renderDetail(); } }, label));
    }
    box.append(tabs);
    const body = h('div', { class: 'tab-body' });
    box.append(body);
    if (S.tab === 'activities') renderActivities(body);
    else if (S.tab === 'logs') renderLogs(body);
    else if (S.tab === 'request') api('GET', `/api/instances/${encodeURIComponent(i.instanceId)}/request`).then((r) => body.append(h('pre', {}, JSON.stringify(r, null, 2)))).catch((e) => body.append(h('div', { class: 'muted' }, e.message)));
    else body.append(
      i.error ? h('div', {}, h('div', { class: 'kv' }, 'error'), h('pre', { class: 'lv-error' }, i.error)) : null,
      h('div', { class: 'kv' }, 'output'), h('pre', {}, JSON.stringify(i.output, null, 2)),
      h('div', { class: 'kv', style: 'margin-top:8px' }, 'vars'), h('pre', {}, JSON.stringify(i.vars, null, 2)),
      h('div', { class: 'kv', style: 'margin-top:8px' }, 'input'), h('pre', {}, JSON.stringify(i.input, null, 2)));
  }

  function renderActivities(body) {
    const t = h('table', { class: 'acts' }, h('thead', {}, h('tr', {}, ['Node', 'Status', 'Port', 'Try', 'Started', 'Duration', 'Detail'].map((x) => h('th', {}, x)))));
    const tb = h('tbody');
    for (const a of S.activities) {
      const detail = a.error || (a.output !== undefined ? JSON.stringify(a.output) : '');
      const row = h('tr', { class: 'clickable', onclick: () => {
        if (row.nextSibling?.classList?.contains('exp')) { row.nextSibling.remove(); return; }
        row.after(h('tr', { class: 'exp' }, h('td', { colspan: 7 }, h('pre', {}, JSON.stringify({ input: a.input, output: a.output, error: a.error }, null, 2)))));
      } },
      h('td', {}, a.nodeName || a.nodeId, h('div', { class: 'kv' }, `${a.nodeType}${a.activityId ? ' · ' + a.activityId : ''}`)),
      h('td', { html: chip(a.status) }), h('td', { class: 'kv' }, a.port || ''), h('td', {}, a.attempt || 1),
      h('td', { class: 'kv' }, fmtTime(a.startedAt)), h('td', { class: 'kv' }, a.endedAt ? fmtDur(a.endedAt - a.startedAt) : '…'),
      h('td', { class: a.error ? 'lv-error' : 'kv' }, trunc(detail, 70)));
      tb.append(row);
    }
    t.append(tb); body.append(t);
  }

  async function renderLogs(body) {
    const logs = await api('GET', `/api/instances/${encodeURIComponent(S.selInst)}/logs?limit=500`);
    if (!logs.length) body.append(h('div', { class: 'muted' }, 'No logs (maybe purged by retention).'));
    for (const l of logs) body.append(h('div', { class: 'log' },
      h('span', { class: 'muted' }, new Date(l.ts).toLocaleTimeString(undefined, { hour12: false }) + '.' + String(l.ts % 1000).padStart(3, '0') + ' '),
      h('span', { class: `lv lv-${l.level}` }, l.level.toUpperCase()), l.nodeId ? h('span', { class: 'muted' }, `[${l.nodeId}] `) : null, l.message,
      l.data !== undefined ? h('span', { class: 'muted' }, ' ' + JSON.stringify(l.data)) : null));
  }

  async function control(action) {
    try { await api('POST', `/api/instances/${encodeURIComponent(S.selInst)}/${action}`, {}); toast(`${action} sent`); setTimeout(refreshDetail, 300); }
    catch (e) { toast(e.message, true); }
  }
  function sendSignal() {
    const def = S.wf?.id === S.instDetail?.workflowId ? S.wf : null;
    const names = [...new Set((def?.nodes || []).filter((n) => n.type === 'signal').map((n) => n.config.signalName))];
    dialog('Send signal', [
      { key: 'name', label: 'Signal name', value: names[0] || '', required: true, help: names.length ? `Signals in this workflow: ${names.join(', ')}` : '' },
      { key: 'payload', label: 'Payload (JSON)', type: 'textarea', rows: 5, value: '{\n  "by": "designer"\n}' },
    ], async (v) => {
      let payload; try { payload = v.payload.trim() ? JSON.parse(v.payload) : undefined; } catch (e) { toast(`Payload JSON: ${e.message}`, true); return false; }
      try { await api('POST', `/api/instances/${encodeURIComponent(S.selInst)}/signal`, { name: v.name, payload }); toast(`signal "${v.name}" sent`); }
      catch (e) { toast(e.message, true); return false; }
    }, 'Send');
  }

  // status overlay on the canvas
  function applyOverlay() {
    const i = S.instDetail;
    const on = i && S.wf && i.workflowId === S.wf.id;
    const tag = $('#overlayTag');
    tag.hidden = !on;
    if (on) { tag.innerHTML = ''; tag.append(h('span', {}, 'Showing run '), h('code', {}, i.instanceId), h('span', { html: chip(i.status) }), h('button', { class: 'ghost small', onclick: () => { S.selInst = null; S.instDetail = null; S.activities = []; renderInstances(); renderDetail(); applyOverlay(); } }, '✕')); }
    const latest = {};
    if (on) for (const a of S.activities) latest[a.nodeId] = a.status;
    const colors = { COMPLETED: css('--ok'), FAILED: css('--danger'), RUNNING: css('--accent'), WAITING: css('--wait'), RETRYING: css('--warn'), CANCELLED: css('--muted') };
    for (const [id, g] of views) {
      const body = g.getObjects().find((o) => o.ffRole === 'body');
      const dot = g.getObjects().find((o) => o.ffRole === 'status');
      let st = latest[id];
      if (on && i.currentNodes?.includes(id)) st = i.status === 'WAITING' ? 'WAITING' : 'RUNNING';
      const c = st ? colors[st] || css('--line') : css('--line');
      body.set({ stroke: c, strokeWidth: st ? 2.5 : 1.5, strokeDashArray: st === 'WAITING' || st === 'RUNNING' ? [6, 4] : null });
      dot.set({ fill: st ? c : 'transparent' });
    }
    canvas.requestRenderAll();
  }

  // live updates
  function connectStream() {
    const es = new EventSource('/api/stream');
    const live = $('#live');
    es.onopen = () => { live.classList.add('on'); live.textContent = '● live'; };
    es.onerror = () => { live.classList.remove('on'); live.textContent = '○ reconnecting'; };
    let actTimer;
    es.addEventListener('instance', (m) => {
      const inst = JSON.parse(m.data);
      const all = $('#allWorkflows').checked, sf = $('#statusFilter').value;
      const idx = S.instances.findIndex((x) => x.instanceId === inst.instanceId);
      const matches = (all || inst.workflowId === S.wf?.id) && (!sf || inst.status === sf);
      if (idx >= 0) { if (matches) S.instances[idx] = inst; else S.instances.splice(idx, 1); }
      else if (matches) S.instances.unshift(inst);
      renderInstances();
      if (inst.instanceId === S.selInst) { S.instDetail = { ...S.instDetail, ...inst }; clearTimeout(actTimer); actTimer = setTimeout(refreshDetail, 150); }
    });
    es.addEventListener('activity', (m) => {
      const a = JSON.parse(m.data);
      if (a.instanceId === S.selInst) { clearTimeout(actTimer); actTimer = setTimeout(refreshDetail, 150); }
    });
  }

  // ───────────── wiring ─────────────
  $('#wfSelect').addEventListener('change', (e) => openWorkflow(e.target.value));
  $('#btnNew').onclick = newWorkflow;
  $('#btnSave').onclick = save;
  $('#btnValidate').onclick = validate;
  $('#btnRun').onclick = run;
  $('#btnDelete').onclick = deleteWorkflow;
  $('#btnSettings').onclick = settings;
  $('#btnCluster').onclick = showCluster;
  $('#btnZoomIn').onclick = () => zoomBy(1.2);
  $('#btnZoomOut').onclick = () => zoomBy(1 / 1.2);
  $('#btnFit').onclick = fit;
  $('#btnRefresh').onclick = loadInstances;
  $('#allWorkflows').onchange = loadInstances;
  $('#statusFilter').onchange = loadInstances;
  $('#drawerToggle').onclick = () => { const d = $('#drawer'); d.classList.toggle('collapsed'); $('#drawerToggle').textContent = (d.classList.contains('collapsed') ? '▸' : '▾') + ' Instances'; };
  window.addEventListener('beforeunload', (e) => { if (S.dirty) { e.preventDefault(); e.returnValue = ''; } });
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => renderAll());

  window.__flowforge = { state: S, canvas }; // handy for debugging / UI tests

  (async function boot() {
    try { const t = localStorage.getItem('ff.theme'); if (t && t !== 'auto') document.documentElement.dataset.theme = t; } catch {}
    const meta = await api('GET', '/api/node-types');
    S.types = meta.nodeTypes; S.triggerTypes = meta.triggerTypes;
    renderPalette();
    resize();
    let last; try { last = localStorage.getItem('ff.lastWf'); } catch {}
    await loadWorkflows(last);
    const id = $('#wfSelect').value;
    if (id) await openWorkflow(id); else renderInspector();
    connectStream();
    document.fonts?.ready.then(() => renderAll());
  })().catch((e) => toast(`Failed to load: ${e.message}`, true));
})();
