import {
  BitmapLayer,
  Deck,
  LineLayer,
  PolygonLayer,
  ScatterplotLayer,
  TileLayer,
} from 'deck.gl';

const Observable = (initialValue) => {
  let value = initialValue;
  const subscribers = new Set();
  return {
    get: () => value,
    set: (newValue) => {
      if (value === newValue) return;
      value = newValue;
      subscribers.forEach((fn) => fn(value));
    },
    subscribe: (fn, options = { immediate: true }) => {
      subscribers.add(fn);
      if (options.immediate) fn(value);
      return () => subscribers.delete(fn);
    },
  };
};

const createStore = () => ({
  stations: Observable([]),
  edge_index: Observable({}),
  click_info: Observable({}),
  selected_rows: Observable([]),
  selected_cols: Observable([]),
  width: Observable(560),
  height: Observable(800),
  debug: Observable(false),
  focus: Observable(''),
  highlights: Observable([]),
  edges: Observable([]),
  // What *kind* of selection produced the current focus/highlights/edges.
  // Drives the rides simulator's focused-mode behaviour:
  //   null            — no selection, full ambient walk
  //   'station'       — a single station was clicked (map or row/col label)
  //   'mat_cell'      — a single matrix cell click (one origin → one dest)
  //   'col_dendro'    — selected origins (outgoing rides only)
  //   'row_dendro'    — selected destinations (incoming rides only)
  //   'cat_value'     — manual category selection (treated like col_dendro
  //                      since the natural interpretation is "rides
  //                      starting from any of these stations")
  selection_kind: Observable(null),
  deck_check: Observable({ inputs: true, computed: true, layers: true }),
  deck_ready: Observable(false),
  palette_rgb: Observable([]),
  matrix_axis_slice: Observable({}),
  spatial_mix: Observable(0),
  hovered_cluster: Observable(null),
  // Pre-computed alpha-shape neighborhoods keyed by cluster_id, sliced by alpha_index.
  // See nbhd.compute_cluster_alpha_shapes for the wire format.
  cluster_polygons: Observable({ levels_miles: [], polygons: [] }),
  alpha_index: Observable(4),
  show_neighborhoods: Observable(true),
  show_stations: Observable(true),
  // Persistent "pinned" cluster — survives slider changes, cluster-resolution
  // tweaks, mouse leaves. Toggled by clicking an NBHD polygon.
  pinned_cluster: Observable(null),
  // Front-end-only display multipliers driven by topbar sliders. Cosmetic, not
  // synced to Python traitlets — they tweak the visual treatment without
  // changing the semantic state of the map.
  station_size_mult: Observable(1.0),
  nbhd_opacity_mult: Observable(0.4),
  // Animated bike-ride simulation. transition_topk is a sparse top-K
  // destination distribution per origin (shipped from Python);
  // station_outflow holds raw per-station outflow counts (also from
  // Python, when raw trips are available) used to weight initial bike
  // placements and rebalancing teleports by true trip volume;
  // show_rides toggles the layer; current_time is the wall-clock used
  // by the random walker (it advances every animation frame while
  // rides are visible).
  transition_topk: Observable({}),
  station_outflow: Observable({}),
  show_rides: Observable(true),
  // Slider-driven count of simultaneously animated rides. Drives the
  // ambient pool size directly via `targetRidesPoolSize`. Range [1, 20000].
  n_rides: Observable(10000),
  current_time: Observable(0),
});

const log = (store, ...args) => {
  if (store.debug.get()) console.log('[bike-map]', ...args);
};

// Map station click: how many neighbors per direction (row + col slices)
// to ask the Clustergram for. The simulated rides carry the long-tail
// signal visually, so the lines themselves stay focused on the dominant
// flows — top-30 keeps the geometry readable instead of devolving into
// spaghetti when a Manhattan hub has hundreds of non-trivial neighbors.
const MAP_STATION_SLICE_TOP_K = 30;

/**
 * Ask the Clustergram for a normal row-axis + col-axis slice (same station on both axes).
 */
function pushRowColMatrixSliceRequest(model, rowIndex, colIndex) {
  if (!model?.set) return;
  const req_id =
    typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `r${Date.now()}-${Math.random().toString(16).slice(2)}`;
  model.set('matrix_slice_request_out', {});
  model.set('matrix_slice_request_out', {
    req_id,
    op: 'row_col',
    row_index: Number(rowIndex),
    col_index: Number(colIndex),
    max_entries: MAP_STATION_SLICE_TOP_K,
  });
  model.save_changes();
}

function findAxisIndex(names, raw) {
  const want = String(raw || '').trim();
  if (!want || !Array.isArray(names)) return -1;
  let i = names.findIndex((x) => String(x).trim() === want);
  if (i >= 0) return i;
  if (want.includes('|')) {
    const rhs = want.split('|', 2)[1].trim();
    i = names.findIndex((x) => String(x).trim() === rhs);
    if (i >= 0) return i;
  }
  return -1;
}

const sameList = (a, b) => {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) if (a[i] !== b[i]) return false;
  return true;
};

const setDerivedState = (store, { focus, highlights, edges, kind }) => {
  const curFocus = store.focus.get() || '';
  const curHighlights = store.highlights.get() || [];
  const curEdges = store.edges.get() || [];
  const curKind = store.selection_kind.get() || null;

  const nextFocus = focus || '';
  const nextHighlights = highlights || [];
  const nextEdges = edges || [];
  // Allow explicit kind=null to mean "clear", and undefined to mean
  // "infer". Inference: focus → 'station', else null. mat_cell and
  // dendro modes always pass kind explicitly.
  const inferredKind = nextFocus ? 'station' : null;
  const nextKind = kind === undefined ? inferredKind : kind;

  let changed = false;
  if (curFocus !== nextFocus) {
    store.focus.set(nextFocus);
    changed = true;
  }
  if (!sameList(curHighlights, nextHighlights)) {
    store.highlights.set(nextHighlights);
    changed = true;
  }
  const curStr = JSON.stringify(curEdges);
  const nextStr = JSON.stringify(nextEdges);
  if (curStr !== nextStr) {
    store.edges.set(nextEdges);
    changed = true;
  }
  if (curKind !== nextKind) {
    store.selection_kind.set(nextKind);
    changed = true;
  }
  return changed;
};

function basemapLayer(opacity = 255) {
  return new TileLayer({
    id: 'bike-basemap',
    data: 'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
    minZoom: 0,
    maxZoom: 19,
    tileSize: 256,
    opacity: opacity / 255,
    renderSubLayers: (props) => {
      if (!props.data || !props.tile) return null;
      let west;
      let south;
      let east;
      let north;
      const bb = props.tile.boundingBox;
      if (bb && bb.length === 2) {
        [[west, south], [east, north]] = bb;
      } else if (props.tile.bbox && 'west' in props.tile.bbox) {
        ({ west, south, east, north } = props.tile.bbox);
      } else {
        return null;
      }
      return new BitmapLayer(props, {
        id: `${props.id}-bitmap`,
        data: [0],
        image: props.data,
        bounds: [west, south, east, north],
      });
    },
  });
}

/** Okabe-Ito (colorblind-safe) first, then Celldega `clust.constants._COLOR_PALETTE` - keep in sync with notebook `STATION_PALETTE_HEX`. */
const STATION_PALETTE_HEX = [
  '#E69F00', '#56B4E9', '#009E73', '#F0E442', '#0072B2', '#D55E00', '#CC79A7',
  '#393b79', '#aec7e8', '#ff7f0e', '#ffbb78', '#98df8a', '#bcbd22', '#404040',
  '#ff9896', '#c5b0d5', '#8c5648', '#1f77b4', '#5254a3', '#FFDB58', '#c49c94',
  '#e377c2', '#7f7f7f', '#2ca02c', '#9467bd', '#dbdb8d', '#17becf', '#637939',
  '#6b6ecf', '#9c9ede', '#d62728', '#8ca252', '#8c6d31', '#bd9e39', '#e7cb94',
  '#843c39', '#ad494a', '#d6616b', '#7b4173', '#a55194', '#ce6dbd', '#de9ed6',
];

function hexToRgb(hex) {
  const h = String(hex).replace('#', '');
  if (h.length !== 6) return [145, 150, 170];
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
}

const DEFAULT_PALETTE_RGB = STATION_PALETTE_HEX.map(hexToRgb);

function resolvePaletteRgb(store) {
  const raw = store.palette_rgb.get();
  if (Array.isArray(raw) && raw.length > 0) {
    return raw.map((row) => {
      if (Array.isArray(row) && row.length >= 3) {
        return [Number(row[0]), Number(row[1]), Number(row[2])];
      }
      return [145, 150, 170];
    });
  }
  return DEFAULT_PALETTE_RGB;
}

/** cluster_id 0 = unclustered / unknown (matches Clustergram label "0"). */
const CLUSTER_UNKNOWN_RGB = [176, 180, 190];

/** Traffic view: stations with no edge to the focused hub. */
const TRAFFIC_UNCONNECTED_RGBA = [168, 172, 180, 118];

function clusterRgbFromId(clusterId, paletteRgb) {
  const id = Number(clusterId);
  if (!Number.isFinite(id) || id <= 0) return CLUSTER_UNKNOWN_RGB;
  const pal = paletteRgb.length ? paletteRgb : DEFAULT_PALETTE_RGB;
  const c = pal[(id - 1) % pal.length];
  return [c[0], c[1], c[2]];
}

function clusterFillWithAlpha(clusterId, alpha, paletteRgb) {
  const [r, g, b] = clusterRgbFromId(clusterId, paletteRgb);
  return [r, g, b, alpha];
}

function clusterFillColor(clusterId, paletteRgb) {
  return clusterFillWithAlpha(clusterId, 228, paletteRgb);
}

/** Same endpoints as LineLayer getColor: out red, in blue. */
const FLOW_OUT_RGB = [255, 70, 70];
const FLOW_IN_RGB = [70, 150, 255];

function flowBlendRgb(outW, inW) {
  const o = Math.max(0, Number(outW) || 0);
  const i = Math.max(0, Number(inW) || 0);
  const sum = o + i;
  if (sum <= 0) return [145, 150, 170];
  const t = o / sum;
  const lerp = (a, b, x) => Math.round(a + (b - a) * x);
  return [
    lerp(FLOW_IN_RGB[0], FLOW_OUT_RGB[0], t),
    lerp(FLOW_IN_RGB[1], FLOW_OUT_RGB[1], t),
    lerp(FLOW_IN_RGB[2], FLOW_OUT_RGB[2], t),
  ];
}

function flowBlendWithAlpha(outW, inW, alpha) {
  const [r, g, b] = flowBlendRgb(outW, inW);
  return [r, g, b, alpha];
}

// ---------- Ride simulator ----------------------------------------------
// Animates ~1000 bike rides on the map as a brownian-style random walk
// over the transition graph: each ride remembers its current station and
// hops to a new one drawn from that station's top-K destination
// distribution. When it arrives, it samples again from the new station's
// distribution — the rides effectively trace markov-chain trajectories
// of the empirical bike-flow process. The whole thing is front-end
// driven; Python only ships the sparse top-K table once at widget
// creation so this works in fully static HTML embeds.

// Ride pool sizing. The user-facing "Rides" slider (1..N_RIDES_MAX,
// default N_RIDES_DEFAULT) sets the ambient pool size directly via the
// `n_rides` traitlet. Three selection regimes:
//
//   - narrow ('station' / 'mat_cell'): shrink to RIDES_FOCUSED_FRACTION
//     of the slider value (floor RIDES_FOCUSED_MIN). A one-station view
//     doesn't need 10k walkers and the dense swarm would obscure the
//     in/out lines.
//   - dendro / category ('col_dendro' / 'row_dendro' / 'cat_value'):
//     scale linearly by selection_size / total_stations. Selecting all
//     stations behaves like ambient; selecting a small cluster gets a
//     proportionally small swarm (so per-station ride density stays
//     constant whether you click a big or small group).
//   - ambient (no focus): full slider value.
const N_RIDES_MIN = 1;
const N_RIDES_MAX = 20000;
const N_RIDES_DEFAULT = 10000;
const RIDES_FOCUSED_FRACTION = 0.10;
const RIDES_FOCUSED_MIN = 50;
const NARROW_FOCUS_KINDS = new Set(['station', 'mat_cell']);
function targetRidesPoolSize(nRides, kind, focusCtx, totalStations) {
  const n = Math.max(
    N_RIDES_MIN,
    Math.min(N_RIDES_MAX, Math.round(Number(nRides) || N_RIDES_DEFAULT)),
  );
  if (NARROW_FOCUS_KINDS.has(kind)) {
    return Math.max(RIDES_FOCUSED_MIN, Math.round(n * RIDES_FOCUSED_FRACTION));
  }
  if (focusCtx && (focusCtx.mode === 'col_dendro' || focusCtx.mode === 'row_dendro')) {
    const selSize = focusCtx.mode === 'col_dendro'
      ? (focusCtx.origin?.names?.length || 0)
      : (focusCtx.dest?.names?.length || 0);
    const total = Math.max(1, Number(totalStations) || 0);
    if (selSize > 0) {
      const frac = Math.min(1, selSize / total);
      return Math.max(RIDES_FOCUSED_MIN, Math.round(n * frac));
    }
  }
  return n;
}
// Per-segment timing. We use a *constant velocity* model: every ride
// covers the same number of degrees per millisecond on screen, so a
// short hop finishes quickly and a cross-town hop takes proportionally
// longer. This keeps the visible flow speed consistent regardless of
// segment length — without it, short hops crawl (because the duration
// has a base cost) while long hops streak. RIDE_MS_PER_DEG sets that
// constant velocity (~35 s/deg → a typical 0.07° NYC hop takes ~2.5 s).
// A small jitter on the *velocity* keeps dots from arriving in lockstep
// without breaking the constant-speed visual.
const RIDE_MS_PER_DEG = 180000;
const RIDE_SEG_VEL_JITTER = 0.18;
const RIDE_SEG_MIN_MS = 250;
// Initial scatter: when the pool is first seeded (or fully flushed on
// focus change) we randomize each ride's progress so the dots don't all
// depart together — they immediately appear as a steady-state flow.
const RIDE_INITIAL_T_SCATTER = true;
// PageRank-style teleport that mimics how bike-share organizations
// physically rebalance bikes back to high-traffic stations. Without it,
// walkers can spend many steps shuffling between a handful of nearby
// outer-borough stations because `transition_prob` is column-normalized
// and loses absolute trip volume. With probability RIDE_TELEPORT_PROB
// per step a walker instead jumps to a station drawn from the chain's
// stationary distribution (computed by power iteration below), which is
// concentrated on busy hubs — so the swarm continually rebalances back
// to high-density areas, exactly like a redistribution truck would.
const RIDE_TELEPORT_PROB = 0.05;
// Power-iteration parameters for the stationary distribution. 30 steps
// is comfortably enough for a 2k-station chain to converge given the
// fast mixing induced by the teleport above; cost is one-time at
// sampler creation (~5 ms for NYC).
const RIDE_STATIONARY_ITERS = 30;

/**
 * Build a sampler over a {origin: [[dest, weight], ...]} top-K table.
 *
 * Returns:
 *   { origins, sample(currentName?) } where:
 *     - origins: array of valid origin station names (those with a
 *       non-empty outgoing distribution)
 *     - sample(currentName): returns the next destination name based on
 *       the markov kernel rooted at `currentName`. Falls back to a
 *       uniform pick over `origins` when `currentName` is not in the
 *       table or has no outgoing mass.
 *
 * Self-transitions are filtered out at sample time so each step always
 * makes geographic progress.
 */
function makeRideSampler(transitionTopk, stationOutflow) {
  const origins = [];
  const cdfByOrigin = new Map();
  for (const [origin, entries] of Object.entries(transitionTopk || {})) {
    if (!Array.isArray(entries) || entries.length === 0) continue;
    let total = 0;
    const dests = new Array(entries.length);
    const cum = new Float64Array(entries.length);
    for (let i = 0; i < entries.length; i += 1) {
      const e = entries[i];
      const w = Math.max(0, Number(e?.[1]) || 0);
      total += w;
      dests[i] = String(e?.[0] || '');
      cum[i] = total;
    }
    if (total <= 0) continue;
    origins.push(origin);
    cdfByOrigin.set(origin, { dests, cum, total });
  }
  // ---- Volume distribution for initial seed + teleport --------------
  // Two ways to estimate "where bikes are" / "where rides start":
  //   1. Raw outflow counts (preferred): the Python side computed real
  //      per-station trip counts from the trips DataFrame and shipped
  //      them via station_outflow. These are the ground truth.
  //   2. Stationary distribution (fallback): when raw trips weren't
  //      available we approximate by power-iterating the markov chain
  //      defined by the top-K kernel; the result is concentrated on the
  //      same hubs but slightly biased by the topk truncation.
  // Either way we end up with a non-negative weight per origin station,
  // which we then turn into a teleport CDF. The initial seed uses the
  // same CDF (via pickInitial) so initial bikes-per-station tracks
  // either true outflow or its best approximation.
  const N = origins.length;
  const weights = new Float64Array(N);
  const useRawOutflow =
    stationOutflow
    && typeof stationOutflow === 'object'
    && Object.keys(stationOutflow).length > 0;
  if (useRawOutflow) {
    for (let i = 0; i < N; i += 1) {
      const w = Number(stationOutflow[origins[i]]) || 0;
      weights[i] = w > 0 ? w : 0;
    }
  } else {
    // Power iteration over the sparse top-K kernel. π = P · π.
    const idxByName = new Map();
    for (let i = 0; i < N; i += 1) idxByName.set(origins[i], i);
    let pi = new Float64Array(N);
    if (N > 0) pi.fill(1 / N);
    for (let iter = 0; iter < RIDE_STATIONARY_ITERS && N > 0; iter += 1) {
      const next = new Float64Array(N);
      for (let oi = 0; oi < N; oi += 1) {
        const massHere = pi[oi];
        if (massHere <= 0) continue;
        const entry = cdfByOrigin.get(origins[oi]);
        if (!entry) continue;
        const { dests, cum, total } = entry;
        let prevCum = 0;
        for (let kk = 0; kk < dests.length; kk += 1) {
          const w = (cum[kk] - prevCum) / total;
          prevCum = cum[kk];
          const di = idxByName.get(dests[kk]);
          if (di != null) next[di] += w * massHere;
        }
      }
      let sum = 0;
      for (let i = 0; i < N; i += 1) sum += next[i];
      if (sum <= 0) break;
      for (let i = 0; i < N; i += 1) pi[i] = next[i] / sum;
    }
    for (let i = 0; i < N; i += 1) weights[i] = pi[i];
  }
  // Build the teleport CDF from `weights`. Only keep stations that also
  // have an outgoing entry, so teleport always lands somewhere with
  // onward edges (avoids visual stalls).
  const teleportNames = [];
  let teleportTotal = 0;
  const teleportCum = [];
  for (let i = 0; i < N; i += 1) {
    const w = weights[i];
    if (w <= 0) continue;
    teleportTotal += w;
    teleportNames.push(origins[i]);
    teleportCum.push(teleportTotal);
  }
  const pickFromCum = (names, cum, total, exclude) => {
    if (!names.length || total <= 0) return null;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const u = Math.random() * total;
      let pick = names[names.length - 1];
      for (let i = 0; i < cum.length; i += 1) {
        if (u < cum[i]) {
          pick = names[i];
          break;
        }
      }
      if (pick !== exclude) return pick;
    }
    return names[0] !== exclude ? names[0] : (names[1] || names[0]);
  };
  const pickFrom = (entry, exclude) => {
    if (!entry) return null;
    return pickFromCum(entry.dests, entry.cum, entry.total, exclude);
  };
  const teleport = (exclude) => pickFromCum(teleportNames, teleportCum, teleportTotal, exclude);
  return {
    origins,
    /** Teleport-weighted initial pick — used to seed the pool. */
    pickInitial() {
      if (teleportTotal > 0) return teleport(null);
      if (origins.length === 0) return null;
      return origins[(Math.random() * origins.length) | 0];
    },
    /**
     * One markov step from `currentName`. With probability
     * RIDE_TELEPORT_PROB we ignore the local distribution and teleport
     * to an inflow-weighted random destination — this keeps the walk
     * from collapsing into low-volume cycles in the outer boroughs.
     */
    sampleNext(currentName) {
      if (origins.length === 0) return null;
      if (Math.random() < RIDE_TELEPORT_PROB) {
        const t = teleport(currentName);
        if (t) return t;
      }
      const entry = cdfByOrigin.get(currentName);
      if (entry) return pickFrom(entry, currentName);
      // Unknown / no outgoing distribution — fall back to teleport so
      // the ride doesn't get stuck.
      return teleport(currentName)
        || origins[(Math.random() * origins.length) | 0];
    },
  };
}

/**
 * Compute a per-segment duration that scales with hop distance so long
 * trips visibly take longer than short ones. Pure euclidean degrees is
 * fine here — we only need rough proportionality, and at any single
 * city's latitude the lng/lat distortion is roughly constant.
 */
function rideSegmentDuration(fromPos, toPos) {
  const dlng = (toPos[0] - fromPos[0]);
  const dlat = (toPos[1] - fromPos[1]);
  const dist = Math.sqrt(dlng * dlng + dlat * dlat);
  // Constant-velocity duration: dist * MS_PER_DEG, with a small
  // multiplicative jitter on velocity so dots in the same wave don't
  // arrive in lockstep. RIDE_SEG_MIN_MS guards against zero-length
  // segments (co-located stations) becoming instantaneous.
  const jitter = 1 + (Math.random() - 0.5) * 2 * RIDE_SEG_VEL_JITTER;
  return Math.max(RIDE_SEG_MIN_MS, dist * RIDE_MS_PER_DEG * jitter);
}

function pickFromCdfArr(names, cum, total) {
  if (!names.length || total <= 0) return null;
  const u = Math.random() * total;
  for (let i = 0; i < cum.length; i += 1) {
    if (u < cum[i]) return names[i];
  }
  return names[names.length - 1];
}

/**
 * Build a one-shot CDF over a list of station names with arbitrary
 * weights, dropping anything without a valid position. Returns null
 * when nothing usable remains.
 */
function buildWeightedCdf(weighted, posLookup) {
  const names = []; const cum = []; let total = 0;
  for (const [name, weight] of weighted) {
    if (!name || !posLookup[name]) continue;
    const w = Math.max(0, Number(weight) || 0);
    if (w <= 0) continue;
    total += w;
    names.push(name);
    cum.push(total);
  }
  if (total <= 0) return null;
  return { names, cum, total };
}

/**
 * Build a "focused mode" sampling context that the rides simulator uses
 * in place of the ambient Markov walk. Five shapes, all returning the
 * same { mode, ... } envelope so `spawnFocusedRide` can dispatch:
 *
 *   - 'station'    a single clicked station; in/out CDFs from
 *                  store.edges (which the link layer is also drawing)
 *   - 'mat_cell'   a single matrix cell click; every ride is the same
 *                  fixed (from -> to) trip
 *   - 'col_dendro' selected origins; rides start at one of them
 *                  (weighted by station_outflow when available, else
 *                  uniform) and visit a destination drawn from that
 *                  origin's top-K kernel
 *   - 'row_dendro' selected destinations; for each destination we
 *                  invert transition_topk to build an incoming CDF
 *                  over origins, then pick a destination (weighted by
 *                  total inflow into the selection) and an origin
 *                  from that destination's incoming CDF
 *   - 'cat_value'  manual category — same as 'col_dendro'
 *
 * Returns null when no usable rides can be generated (no positions,
 * empty distributions, ...).
 */
function buildFocusedRideContext({
  kind,
  focus,
  highlights,
  edges,
  transitionTopk,
  stationOutflow,
  posLookup,
}) {
  const has = (n) => Boolean(n) && Boolean(posLookup[n]);

  if (kind === 'station') {
    if (!has(focus)) return null;
    const outPairs = [];
    const inPairs = [];
    for (const e of edges || []) {
      const w = Math.max(0, Number(e?.weight) || 0);
      if (w <= 0) continue;
      if (e.direction === 'out' && e.source_name === focus && has(e.target_name)) {
        outPairs.push([e.target_name, w]);
      } else if (e.direction === 'in' && e.target_name === focus && has(e.source_name)) {
        inPairs.push([e.source_name, w]);
      }
    }
    const out = buildWeightedCdf(outPairs, posLookup);
    const inn = buildWeightedCdf(inPairs, posLookup);
    if (!out && !inn) return null;
    return { mode: 'station', focus, out, inn };
  }

  if (kind === 'mat_cell') {
    // store.edges has the single 'direct' edge.
    const e = (edges || []).find((x) => x && x.direction === 'direct');
    if (!e || !has(e.source_name) || !has(e.target_name)) return null;
    return { mode: 'mat_cell', from: e.source_name, to: e.target_name };
  }

  if (kind === 'col_dendro' || kind === 'cat_value') {
    // Outgoing rides from any station in the selection. Origin weight
    // = real outflow (when shipped) so busy origins fire proportionally.
    const sel = (highlights || []).filter(has);
    if (sel.length === 0) return null;
    const useOutflow = stationOutflow && Object.keys(stationOutflow).length > 0;
    const originPairs = sel.map((n) => [n, useOutflow ? (Number(stationOutflow[n]) || 1) : 1]);
    const origin = buildWeightedCdf(originPairs, posLookup);
    if (!origin) return null;
    // Per-origin destination CDF straight from transition_topk (already
    // top-K with weights). Built once at context-construction so spawn
    // is a single CDF lookup per ride.
    const destByOrigin = new Map();
    for (const o of sel) {
      const entries = (transitionTopk || {})[o];
      if (!Array.isArray(entries) || entries.length === 0) continue;
      const cdf = buildWeightedCdf(
        entries
          .filter((row) => row && row[0] !== o) // strip self-loops
          .map((row) => [String(row[0]), Number(row[1]) || 0]),
        posLookup,
      );
      if (cdf) destByOrigin.set(o, cdf);
    }
    if (destByOrigin.size === 0) return null;
    return { mode: 'col_dendro', origin, destByOrigin };
  }

  if (kind === 'row_dendro') {
    // Incoming rides to any station in the selection. transition_topk
    // is keyed by origin -> top-K destinations; invert it once over the
    // *selected* destinations to build an incoming CDF per destination.
    const sel = (highlights || []).filter(has);
    if (sel.length === 0) return null;
    const selSet = new Set(sel);
    const inflowByDest = new Map(); // dest -> array of [origin, weight]
    for (const d of sel) inflowByDest.set(d, []);
    for (const [origin, entries] of Object.entries(transitionTopk || {})) {
      if (!Array.isArray(entries) || entries.length === 0) continue;
      if (!has(origin)) continue;
      for (const row of entries) {
        const dest = String(row?.[0] || '');
        if (!selSet.has(dest) || dest === origin) continue;
        const w = Number(row?.[1]) || 0;
        if (w <= 0) continue;
        inflowByDest.get(dest).push([origin, w]);
      }
    }
    const originByDest = new Map();
    const destPairs = [];
    for (const d of sel) {
      const cdf = buildWeightedCdf(inflowByDest.get(d) || [], posLookup);
      if (!cdf) continue;
      originByDest.set(d, cdf);
      destPairs.push([d, cdf.total]);
    }
    if (destPairs.length === 0) return null;
    const dest = buildWeightedCdf(destPairs, posLookup);
    if (!dest) return null;
    return { mode: 'row_dendro', dest, originByDest };
  }

  return null;
}

/**
 * One-shot ride for focused mode. Dispatches on focusCtx.mode (see
 * buildFocusedRideContext). The returned ride is marked
 * `focused: true` so `advanceRides` retires it on segment completion
 * instead of taking another Markov step.
 */
function spawnFocusedRide(focusCtx, posLookup, paletteRgb, stationCluster) {
  let from = null; let to = null;

  if (focusCtx.mode === 'station') {
    const { focus, out, inn } = focusCtx;
    const outTotal = out ? out.total : 0;
    const inTotal = inn ? inn.total : 0;
    const total = outTotal + inTotal;
    if (total <= 0) return null;
    const goingOut = (Math.random() * total) < outTotal;
    if (goingOut && out) {
      from = focus;
      to = pickFromCdfArr(out.names, out.cum, out.total);
    } else if (inn) {
      from = pickFromCdfArr(inn.names, inn.cum, inn.total);
      to = focus;
    }
  } else if (focusCtx.mode === 'mat_cell') {
    from = focusCtx.from;
    to = focusCtx.to;
  } else if (focusCtx.mode === 'col_dendro') {
    const o = pickFromCdfArr(focusCtx.origin.names, focusCtx.origin.cum, focusCtx.origin.total);
    if (!o) return null;
    const cdf = focusCtx.destByOrigin.get(o);
    if (!cdf) return null;
    from = o;
    to = pickFromCdfArr(cdf.names, cdf.cum, cdf.total);
  } else if (focusCtx.mode === 'row_dendro') {
    const d = pickFromCdfArr(focusCtx.dest.names, focusCtx.dest.cum, focusCtx.dest.total);
    if (!d) return null;
    const cdf = focusCtx.originByDest.get(d);
    if (!cdf) return null;
    from = pickFromCdfArr(cdf.names, cdf.cum, cdf.total);
    to = d;
  }

  if (!from || !to || !posLookup[from] || !posLookup[to]) return null;
  const cid = stationCluster?.get(from);
  return {
    from_name: from,
    to_name: to,
    t: RIDE_INITIAL_T_SCATTER ? Math.random() : 0,
    duration: rideSegmentDuration(posLookup[from], posLookup[to]),
    color: clusterRgbFromId(cid != null ? cid : 0, paletteRgb),
    position: [posLookup[from][0], posLookup[from][1]],
    focused: true,
  };
}

/** Build a fresh ride from scratch, optionally rooted at `seedName`. */
function spawnRide(sampler, seedName, posLookup, paletteRgb, stationCluster) {
  const start = (seedName && posLookup[seedName]) ? seedName : sampler.pickInitial();
  if (!start) return null;
  const next = sampler.sampleNext(start);
  if (!next || !posLookup[start] || !posLookup[next]) return null;
  const cid = stationCluster?.get(start);
  return {
    from_name: start,
    to_name: next,
    // `t` is the progress along the current segment in [0, 1]. We
    // scatter the initial value so 1000 rides appear as a steady stream
    // rather than all departing in lockstep.
    t: RIDE_INITIAL_T_SCATTER ? Math.random() : 0,
    duration: rideSegmentDuration(posLookup[start], posLookup[next]),
    color: clusterRgbFromId(cid != null ? cid : 0, paletteRgb),
    // Live position; recomputed each frame from posLookup so rides morph
    // alongside stations when the Spatial↔UMAP slider moves.
    position: [posLookup[start][0], posLookup[start][1]],
  };
}

/**
 * Advance every ride by `dtMs`. When a ride completes its segment
 * (t >= 1), it takes a markov step: the destination becomes its new
 * "from" station and a fresh next station is sampled from there.
 *
 * Mutates rides in place; positions are recomputed by linear
 * interpolation in the *current* posLookup so rides ride along with
 * the spatial morph.
 */
function advanceRides(pool, dtMs, sampler, posLookup, paletteRgb, stationCluster, focusCtx) {
  for (let i = 0; i < pool.length; i += 1) {
    const r = pool[i];
    if (!r) continue;
    const fromPos = posLookup[r.from_name];
    let toPos = posLookup[r.to_name];
    if (!fromPos || !toPos) {
      // Endpoint went missing (data swap mid-flight) — drop and let
      // refillRides respawn appropriately for the current mode.
      pool[i] = null;
      continue;
    }
    r.t += dtMs / Math.max(50, r.duration);
    if (r.t >= 1) {
      if (r.focused) {
        // Focused mode: a one-shot trip to/from the focused station. We
        // do NOT continue the chain — the dot completes its segment
        // and disappears. refillRides will spawn a replacement which
        // again starts or ends at the current focus.
        pool[i] = null;
        continue;
      }
      while (r.t >= 1) {
        // Ambient Markov step: arrived at `to_name`; pick next dest.
        const overshoot = r.t - 1;
        r.from_name = r.to_name;
        const nextName = sampler.sampleNext(r.from_name) || sampler.pickInitial();
        if (!nextName || !posLookup[nextName]) {
          pool[i] = null;
          break;
        }
        r.to_name = nextName;
        const newFrom = posLookup[r.from_name];
        const newTo = posLookup[r.to_name];
        r.duration = rideSegmentDuration(newFrom, newTo);
        const cid = stationCluster?.get(r.from_name);
        r.color = clusterRgbFromId(cid != null ? cid : 0, paletteRgb);
        r.t = overshoot;
      }
    }
    if (!pool[i]) continue;
    const cur = pool[i];
    const fp = posLookup[cur.from_name];
    const tp = posLookup[cur.to_name];
    if (!fp || !tp) continue;
    const tt = Math.max(0, Math.min(1, cur.t));
    cur.position[0] = fp[0] * (1 - tt) + tp[0] * tt;
    cur.position[1] = fp[1] * (1 - tt) + tp[1] * tt;
  }
}

/**
 * Top up empty slots in the pool. Used after a flush (e.g. focus
 * change) and during the initial seed.
 */
function refillRides(pool, sampler, seedName, posLookup, paletteRgb, stationCluster, focusCtx) {
  for (let i = 0; i < pool.length; i += 1) {
    if (pool[i]) continue;
    const fresh = focusCtx
      ? spawnFocusedRide(focusCtx, posLookup, paletteRgb, stationCluster)
      : spawnRide(sampler, seedName, posLookup, paletteRgb, stationCluster);
    if (fresh) pool[i] = fresh;
  }
}

function fitViewState(stations) {
  if (!stations.length) return { longitude: -73.98, latitude: 40.75, zoom: 10.5, pitch: 0, bearing: 0 };
  let minLat = Infinity; let maxLat = -Infinity; let minLng = Infinity; let maxLng = -Infinity;
  for (const s of stations) {
    const lat = Number(s.lat); const lng = Number(s.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    minLat = Math.min(minLat, lat); maxLat = Math.max(maxLat, lat);
    minLng = Math.min(minLng, lng); maxLng = Math.max(maxLng, lng);
  }
  const latitude = (minLat + maxLat) / 2;
  const longitude = (minLng + maxLng) / 2;
  const span = Math.max(maxLat - minLat, maxLng - minLng, 0.02);
  const zoom = Math.min(14, Math.max(10, 13 - Math.log2(span * 80)));
  return { latitude, longitude, zoom, pitch: 0, bearing: 0 };
}

function render({ model, el }) {
  const store = createStore();

  // Layout: vertical stack with a thin top toolbar above the deck.gl canvas.
  // Keeping controls *outside* the canvas (rather than overlaid on it) means
  // hovering or dragging the sliders can't drive the map's pointer state, and
  // it mirrors the toolbar pattern used by the Celldega widgets.
  const root = document.createElement('div');
  root.style.cssText =
    'background:#e2e4e8;border-radius:4px;overflow:hidden;display:flex;flex-direction:column;';
  el.appendChild(root);

  // Topbar = three horizontal sections, each a vertical stack of two controls.
  //   [ Toggles ]  |  [ Station/Spatial sliders ]  |  [ NBHD sliders ]
  //   NBHD on top     Spatial ↔ UMAP on top         Radius on top
  //   Stations below  Size below                    Opacity below
  // Grouping by domain (toggles vs station-related sliders vs nbhd-related
  // sliders) keeps related controls visually adjacent and lets each row
  // share a consistent label width within its column.
  const TOPBAR_ROW_HEIGHT = 20;
  // Buttons are smaller than slider rows — a slider needs vertical room
  // for its thumb, a button doesn't. Toggle column has 3 stacked
  // buttons; slider columns now hold up to 3 rows (Spatial↔UMAP / Size /
  // Rides in the station column; Radius / Opacity in the NBHD column).
  // The topbar is sized to fit 3 slider rows + gap + padding; the toggle
  // column distributes its 3 buttons via justify-content: space-between
  // so they spread evenly across the same height.
  const TOGGLE_BUTTON_HEIGHT = 18;
  const TOPBAR_HEIGHT = 78;
  const topbar = document.createElement('div');
  topbar.style.cssText =
    'flex:0 0 auto;height:' + TOPBAR_HEIGHT + 'px;box-sizing:border-box;' +
    'display:flex;flex-direction:row;align-items:stretch;gap:12px;padding:4px 10px;' +
    'background:#f5f6f8;border-bottom:1px solid #d0d3d8;' +
    'font:12px system-ui,sans-serif;color:#333;user-select:none;';
  root.appendChild(topbar);

  // makeColumn: a vertical stack section. `flex` is the CSS flex value so we
  // can keep the toggle column compact (`0 0 auto`) and let the slider
  // columns share remaining width.
  const makeColumn = (flex) => {
    const col = document.createElement('div');
    col.style.cssText =
      'display:flex;flex-direction:column;justify-content:space-between;' +
      'gap:2px;flex:' + flex + ';min-width:0;';
    topbar.appendChild(col);
    return col;
  };

  // Vertical divider between sections.
  const divider = () => {
    const d = document.createElement('div');
    d.style.cssText = 'width:1px;align-self:stretch;background:#d0d3d8;flex:0 0 auto;';
    return d;
  };

  const colToggles = makeColumn('0 0 auto');
  topbar.appendChild(divider());
  const colStation = makeColumn('1 1 0');
  topbar.appendChild(divider());
  const colNbhd = makeColumn('1 1 0');

  const mapHolder = document.createElement('div');
  mapHolder.style.cssText =
    'flex:1 1 auto;position:relative;background:#e2e4e8;transition:background-color 200ms;';
  root.appendChild(mapHolder);

  const styleToggleButton = (btn, on) => {
    btn.style.background = on ? '#1f77b4' : '#fff';
    btn.style.color = on ? '#fff' : '#444';
    btn.style.borderColor = on ? '#1f77b4' : '#ccc';
  };

  // Toggle buttons live in the compact left column, stacked vertically and
  // sharing a fixed width so they read as a small switchboard.
  const TOGGLE_WIDTH = 64;
  const makeToggle = (label, observable, modelKey) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = label;
    btn.style.cssText =
      'width:' + TOGGLE_WIDTH + 'px;height:' + TOGGLE_BUTTON_HEIGHT + 'px;' +
      'padding:0 6px;border:1px solid #ccc;border-radius:3px;' +
      'background:#fff;color:#444;font:11px system-ui,sans-serif;' +
      'line-height:1;cursor:pointer;transition:all 0.18s;box-sizing:border-box;';
    styleToggleButton(btn, observable.get());
    btn.addEventListener('click', () => {
      const next = !observable.get();
      observable.set(next);
      styleToggleButton(btn, next);
      if (modelKey) {
        model.set(modelKey, next);
        model.save_changes();
      }
      scheduleRender();
    });
    observable.subscribe((v) => styleToggleButton(btn, v), { immediate: false });
    return btn;
  };

  colToggles.appendChild(makeToggle('NBHD', store.show_neighborhoods, 'show_neighborhoods'));
  colToggles.appendChild(makeToggle('Stations', store.show_stations, 'show_stations'));
  colToggles.appendChild(makeToggle('Rides', store.show_rides, 'show_rides'));

  // Every slider row uses the same label width and the same fixed
  // input-track width so the bars line up perfectly across columns and
  // visually read as a single switchboard. Label width is sized to the
  // longest label ("Spatial ↔ UMAP"); shorter labels left-align inside.
  const SLIDER_LABEL_W = 88;
  const SLIDER_INPUT_W = 120;

  // makeSliderRow: a horizontal label + range row. No value readout —
  // the slider position itself is the affordance for these cosmetic
  // knobs (Size, Opacity, Rides count) and even Radius is fine without
  // it since the user can see the polygon size change live.
  const makeSliderRow = (label) => {
    const row = document.createElement('div');
    row.style.cssText =
      'display:flex;align-items:center;gap:6px;height:' + TOPBAR_ROW_HEIGHT + 'px;min-width:0;';
    const labEl = document.createElement('span');
    labEl.style.cssText =
      'color:#444;font-weight:500;white-space:nowrap;flex:0 0 ' + SLIDER_LABEL_W + 'px;';
    labEl.textContent = label;
    const inp = document.createElement('input');
    inp.type = 'range';
    inp.style.cssText =
      'flex:0 0 ' + SLIDER_INPUT_W + 'px;width:' + SLIDER_INPUT_W + 'px;cursor:pointer;margin:0;';
    row.appendChild(labEl);
    row.appendChild(inp);
    return { row, input: inp, valEl: null };
  };

  // ---- Station column: Spatial ↔ UMAP morph (top), Station size (bottom) ----
  const spatial = makeSliderRow('Spatial \u2194 UMAP');
  spatial.input.min = '0'; spatial.input.max = '1'; spatial.input.step = '0.01'; spatial.input.value = '0';
  const slider = spatial.input; // alias kept for downstream syncFromModel writer
  spatial.input.addEventListener('input', () => {
    const t = parseFloat(spatial.input.value);
    store.spatial_mix.set(t);
    model.set('spatial_mix', t);
    model.save_changes();
  });
  colStation.appendChild(spatial.row);
  store.stations.subscribe((stations) => {
    const hasUmap = stations.some((s) => s.umap_lng != null && s.umap_lat != null);
    spatial.row.style.visibility = hasUmap ? 'visible' : 'hidden';
  }, { immediate: true });

  const sizeRow = makeSliderRow('Size');
  sizeRow.input.min = '0.4'; sizeRow.input.max = '2.5'; sizeRow.input.step = '0.05';
  sizeRow.input.value = String(store.station_size_mult.get());
  sizeRow.input.addEventListener('input', () => {
    store.station_size_mult.set(parseFloat(sizeRow.input.value));
    scheduleRender();
  });
  colStation.appendChild(sizeRow.row);
  store.show_stations.subscribe((on) => {
    sizeRow.row.style.opacity = on ? '1' : '0.4';
    sizeRow.input.disabled = !on;
  }, { immediate: true });

  // ---- NBHD column: alpha-shape Radius (top), Opacity (bottom) ----
  // Radius slider indexes into cluster_polygons.levels_miles. Hidden when
  // no neighborhoods are precomputed for this city.
  const radiusRow = makeSliderRow('Radius');
  radiusRow.input.min = '0'; radiusRow.input.max = '0'; radiusRow.input.step = '1'; radiusRow.input.value = '0';
  const refreshNbhdSlider = () => {
    const cp = store.cluster_polygons.get() || {};
    const levels = Array.isArray(cp.levels_miles) ? cp.levels_miles : [];
    if (!levels.length) {
      radiusRow.row.style.visibility = 'hidden';
      return;
    }
    radiusRow.row.style.visibility = 'visible';
    radiusRow.input.max = String(levels.length - 1);
    const idx = Math.max(0, Math.min(levels.length - 1, store.alpha_index.get() | 0));
    radiusRow.input.value = String(idx);
  };
  radiusRow.input.addEventListener('input', () => {
    const idx = parseInt(radiusRow.input.value, 10) | 0;
    store.alpha_index.set(idx);
    refreshNbhdSlider();
    model.set('alpha_index', idx);
    model.save_changes();
    scheduleRender();
  });
  colNbhd.appendChild(radiusRow.row);
  store.cluster_polygons.subscribe(refreshNbhdSlider, { immediate: true });
  store.alpha_index.subscribe(refreshNbhdSlider, { immediate: false });
  store.show_neighborhoods.subscribe((on) => {
    radiusRow.row.style.opacity = on ? '1' : '0.4';
    radiusRow.input.disabled = !on;
  }, { immediate: true });

  // Opacity slider: full range 0 → 1 so the user can make NBHDs entirely
  // transparent or fully opaque. Default 0.4 lands on the subtle look that
  // works well as a starting point.
  const opacityRow = makeSliderRow('Opacity');
  opacityRow.input.min = '0'; opacityRow.input.max = '1'; opacityRow.input.step = '0.05';
  opacityRow.input.value = String(store.nbhd_opacity_mult.get());
  opacityRow.input.addEventListener('input', () => {
    store.nbhd_opacity_mult.set(parseFloat(opacityRow.input.value));
    scheduleRender();
  });
  colNbhd.appendChild(opacityRow.row);
  store.show_neighborhoods.subscribe((on) => {
    opacityRow.row.style.opacity = on ? '1' : '0.4';
    opacityRow.input.disabled = !on;
  }, { immediate: true });

  // ---- Rides count slider (1..10000, default 5000) ----
  // Stacked under Spatial↔UMAP / Size in the Station column — the
  // simulated rides live on the station network, so this groups
  // naturally with the other station-related knobs. Drives the ambient
  // pool size and mirrors to the `n_rides` Python traitlet. Narrow
  // selections (single station / matrix cell) shrink the live pool to
  // ~10% of this value; broad selections (dendrogram / category) keep
  // the full count.
  const ridesCountRow = makeSliderRow('Rides');
  ridesCountRow.input.min = String(N_RIDES_MIN);
  ridesCountRow.input.max = String(N_RIDES_MAX);
  ridesCountRow.input.step = '200';
  const refreshRidesCountSlider = () => {
    const v = Math.max(N_RIDES_MIN, Math.min(N_RIDES_MAX, Number(store.n_rides.get()) | 0));
    ridesCountRow.input.value = String(v);
  };
  ridesCountRow.input.addEventListener('input', () => {
    const v = Math.max(N_RIDES_MIN, Math.min(N_RIDES_MAX, parseInt(ridesCountRow.input.value, 10) | 0));
    store.n_rides.set(v);
    model.set('n_rides', v);
    model.save_changes();
    scheduleRender();
  });
  colStation.appendChild(ridesCountRow.row);
  store.n_rides.subscribe(refreshRidesCountSlider, { immediate: true });
  store.show_rides.subscribe((on) => {
    ridesCountRow.row.style.opacity = on ? '1' : '0.4';
    ridesCountRow.input.disabled = !on;
  }, { immediate: true });

  // Map background fades from the basemap-matched grey toward pure white as we
  // morph into UMAP space (where the basemap tiles fade out and a flat white
  // canvas reads cleaner).
  store.spatial_mix.subscribe((mix) => {
    const t = Math.max(0, Math.min(1, Number(mix) || 0));
    const r = Math.round(226 + (255 - 226) * t);
    const g = Math.round(228 + (255 - 228) * t);
    const b = Math.round(232 + (255 - 232) * t);
    mapHolder.style.background = `rgb(${r},${g},${b})`;
  }, { immediate: true });

  mapHolder.addEventListener('mouseleave', () => {
    clearTimeout(hoverTimer);
    if (store.hovered_cluster.get() != null) {
      store.hovered_cluster.set(null);
      scheduleRender();
    }
  });

  let hoverTimer = 0;

  let deck = null;
  let raf = 0;
  let renderVersion = 0;
  let lastActionKey = null;
  let lastActionSeq = -1;
  /**
   * Increments on each `click_info` comm update from the Clustergram (including the null-then-set pair).
   * Used for label/mat/cat toggle detection. Intentionally not tied to `matrix_slice_result` alone so
   * slice-only updates do not spuriously toggle the map off.
   */
  let linkInteractionSeq = 0;
  let linkSnap = { ci: '', rows: '', cols: '', sl: '' };

  const getLinkedWeights = () => {
    const focus = store.focus.get();
    const out = new Map();
    const inn = new Map();
    if (!focus) return { out, inn };
    for (const e of store.edges.get() || []) {
      if (e.direction === 'out' && e.source_name === focus) {
        out.set(e.target_name, Number(e.weight) || 0);
      } else if (e.direction === 'in' && e.target_name === focus) {
        inn.set(e.source_name, Number(e.weight) || 0);
      }
    }
    return { out, inn };
  };

  const computeStateFromInputs = () => {
    const info = store.click_info.get() || {};
    const t = String(info.type || '').replace('-', '_');
    const v = info.value || {};
    const idx = store.edge_index.get() || {};
    const seq = linkInteractionSeq;
    log(store, 'compute start', t, 'rows', (store.selected_rows.get()||[]).length, 'cols', (store.selected_cols.get()||[]).length);

    const setState = (focus, highlights, edges) =>
      setDerivedState(store, { focus, highlights, edges });

    const stationFrom = (raw) => {
      const txt = String(raw || '').trim();
      if (idx[txt]) return txt;
      if (txt.includes('|')) {
        const rhs = txt.split('|', 2)[1].trim();
        if (idx[rhs]) return rhs;
      }
      return txt;
    };

    const coordMap = {};
    for (const s of store.stations.get() || []) {
      const k = stationFrom(String(s.name || ''));
      if (k && s.lng != null && s.lat != null) {
        coordMap[k] = [Number(s.lng), Number(s.lat)];
      }
    }
    const acoord = (n) => (n && idx[n]?.coord) || (n ? coordMap[n] : undefined);

    if (t === 'row_label' || t === 'col_label') {
      const name = stationFrom(v.name);
      const actionKey = `${t}:${name}`;
      if (actionKey === lastActionKey && seq !== lastActionSeq) {
        setState('', [], []);
        lastActionKey = null;
        lastActionSeq = seq;
        log(store, 'label toggle off', t, name);
        return;
      }
      const sl = store.matrix_axis_slice.get() || {};
      const entries = sl.entries;
      const rowAxis = t === 'row_label' && sl.slice_kind === 'row_axis';
      const colAxis = t === 'col_label' && sl.slice_kind === 'col_axis';
      if (Array.isArray(entries) && (rowAxis || colAxis)) {
        const primary = stationFrom(sl.primary_name);
        if (primary === name && entries.length) {
          const edges = [];
          for (const e of entries) {
            const other = stationFrom(e.counterpart_name);
            const cs = acoord(other);
            const ct = acoord(name);
            if (!other || !cs || !ct) continue;
            const w = Number(e.value) || 0;
            const geom = Math.sqrt(Math.max(0, w));
            if (rowAxis) {
              edges.push({
                source_name: other,
                target_name: name,
                direction: 'in',
                opacity: Math.max(0.15, Math.min(0.98, w * 3)),
                weight: w,
                geom_share: geom,
                source: cs,
                target: ct,
              });
            } else {
              edges.push({
                source_name: name,
                target_name: other,
                direction: 'out',
                opacity: Math.max(0.15, Math.min(0.98, w * 3)),
                weight: w,
                geom_share: geom,
                source: ct,
                target: cs,
              });
            }
          }
          setState(name, name ? [name] : [], edges);
          lastActionKey = actionKey;
          lastActionSeq = seq;
          log(store, 'label+axis_slice', t, name, sl.slice_kind, 'edges', edges.length);
          return;
        }
      }
      const bucket = idx[name] || { out: [], in: [] };
      const edges = [
        ...(bucket.out || []).map((e) => ({
          source_name: name,
          target_name: e.name,
          direction: 'out',
          opacity: e.opacity,
          weight: e.w,
          geom_share: e.geom_share,
          trips: e.trips,
          source: e.source,
          target: e.target,
        })),
        ...(bucket.in || []).map((e) => ({
          source_name: e.name,
          target_name: name,
          direction: 'in',
          opacity: e.opacity,
          weight: e.w,
          geom_share: e.geom_share,
          trips: e.trips,
          source: e.source,
          target: e.target,
        })),
      ];
      setState(name, name ? [name] : [], edges);
      lastActionKey = actionKey;
      lastActionSeq = seq;
      log(store, 'label', t, name, 'edges', edges.length);
      return;
    }

    if (t === 'mat_value') {
      const row = stationFrom((v.row || {}).name);
      const col = stationFrom((v.col || {}).name);
      const actionKey = `mat:${col}->${row}`;
      if (actionKey === lastActionKey && seq !== lastActionSeq) {
        setDerivedState(store, { focus: '', highlights: [], edges: [], kind: null });
        lastActionKey = null;
        lastActionSeq = seq;
        log(store, 'mat toggle off', col, row);
        return;
      }
      const p = Number(v.value || 0);
      const src = acoord(col);
      const dst = acoord(row);
      const outEdge = (idx[col]?.out || []).find((e) => e.name === row);
      const g = outEdge != null ? Number(outEdge.geom_share) : NaN;
      const geom = Number.isFinite(g) && g > 0 ? g : Math.sqrt(Math.max(0, p));
      const edge = src && dst
        ? [{
          source_name: col,
          target_name: row,
          direction: 'direct',
          opacity: Math.max(0.2, Math.min(0.98, p * 3)),
          weight: p,
          geom_share: geom,
          trips: outEdge?.trips,
          source: src,
          target: dst,
        }]
        : [];
      setDerivedState(store, {
        focus: '',
        highlights: [col, row].filter(Boolean),
        edges: edge,
        kind: 'mat_cell',
      });
      lastActionKey = actionKey;
      lastActionSeq = seq;
      log(store, 'mat', col, '->', row, p);
      return;
    }

    if (t === 'row_dendro' || t === 'col_dendro') {
      const hasStation = (n) => Boolean(n && (idx[n] || coordMap[n]));
      const fromClick = (v.selected_names || []).map(stationFrom).filter(hasStation);
      const rows = (store.selected_rows.get() || []).map(stationFrom).filter(hasStation);
      const cols = (store.selected_cols.get() || []).map(stationFrom).filter(hasStation);

      if (v.is_unselecting || (Array.isArray(v.selected_names) && v.selected_names.length === 0 && rows.length === 0 && cols.length === 0)) {
        setDerivedState(store, { focus: '', highlights: [], edges: [], kind: null });
        lastActionKey = null;
        lastActionSeq = seq;
        log(store, 'dendro clear', t);
        return;
      }

      // Prefer axis traitlets (they represent full live dendrogram selection);
      // click_info.selected_names can be a smaller interim payload.
      const names = t === 'col_dendro'
        ? (cols.length ? cols : (fromClick.length ? fromClick : rows))
        : (rows.length ? rows : (fromClick.length ? fromClick : cols));

      // No actionKey "toggle off" vs linkInteractionSeq here: dendrogram updates can
      // re-send the same selection across frames; toggling off would flicker.
      const actionKey = `${t}:${names.slice().sort().join('|')}`;
      setDerivedState(store, { focus: '', highlights: names, edges: [], kind: t });
      lastActionKey = actionKey;
      lastActionSeq = seq;
      log(store, 'dendro', t, 'names', names.length, 'rows', rows.length, 'cols', cols.length);
      return;
    }

    if (t === 'cat_value') {
      const axis = v.axis;
      const attrIndex = v.attr_index;
      const catVal = v.value;
      const rawNames = Array.isArray(v.node_names) ? v.node_names : [];
      const names = [...new Set(rawNames.map(stationFrom).filter((n) => idx[n] || coordMap[n]))];
      const actionKey = `cat:${axis}:${attrIndex}:${String(catVal)}`;
      if (actionKey === lastActionKey && seq !== lastActionSeq) {
        setDerivedState(store, { focus: '', highlights: [], edges: [], kind: null });
        lastActionKey = null;
        lastActionSeq = seq;
        log(store, 'cat toggle off', axis, catVal);
        return;
      }
      setDerivedState(store, { focus: '', highlights: names, edges: [], kind: 'cat_value' });
      lastActionKey = actionKey;
      lastActionSeq = seq;
      log(store, 'cat', axis, 'value', catVal, 'stations', names.length);
      return;
    }

    const slPair = store.matrix_axis_slice.get() || {};
    if (slPair.slice_kind === 'row_col') {
      const primaryRaw =
        slPair.row_axis?.primary_name ?? slPair.col_axis?.primary_name ?? null;
      if (!primaryRaw) return;
      const focusName = stationFrom(primaryRaw);
      const focusNow = store.focus.get() || '';
      if (focusNow && focusName !== focusNow) {
        log(store, 'row_col slice skip (stale vs map focus)', focusName, focusNow);
        return;
      }
      const incoming = slPair.row_axis?.entries || [];
      const outgoing = slPair.col_axis?.entries || [];
      const edges = [];
      for (const e of incoming) {
        const other = stationFrom(e.counterpart_name);
        const cs = acoord(other);
        const ct = acoord(focusName);
        if (!other || !cs || !ct) continue;
        const w = Number(e.value) || 0;
        const geom = Math.sqrt(Math.max(0, w));
        edges.push({
          source_name: other,
          target_name: focusName,
          direction: 'in',
          opacity: Math.max(0.15, Math.min(0.98, w * 3)),
          weight: w,
          geom_share: geom,
          source: cs,
          target: ct,
        });
      }
      for (const e of outgoing) {
        const other = stationFrom(e.counterpart_name);
        const cs = acoord(focusName);
        const ct = acoord(other);
        if (!other || !cs || !ct) continue;
        const w = Number(e.value) || 0;
        const geom = Math.sqrt(Math.max(0, w));
        edges.push({
          source_name: focusName,
          target_name: other,
          direction: 'out',
          opacity: Math.max(0.15, Math.min(0.98, w * 3)),
          weight: w,
          geom_share: geom,
          source: cs,
          target: ct,
        });
      }
      setState(focusName, focusName ? [focusName] : [], edges);
      lastActionKey = `row_col:${focusName}`;
      lastActionSeq = seq;
      log(store, 'row_col slice', focusName, 'edges', edges.length);
      return;
    }
  };

  // ---------- Ride simulator state (closure-scoped) -------------------
  // Pool of markov-chain walkers. Each walker remembers its current
  // segment (from -> to, t in [0,1]); when t reaches 1 it takes a new
  // step from the destination's distribution. State lives here in the
  // closure so it persists across renders without needing to round-trip
  // through the observable store. Pool length is sized per-city by
  // `targetRidesPoolSize(stationCount)` and resized in place when the
  // station set changes (rare — basically only first render).
  const ridesPool = [];
  let ridesSampler = makeRideSampler({});
  let lastSamplerSig = '';
  let lastFocusForRides = null;
  let lastRidesFrameTs = 0;
  let ridesFrame = 0;
  const resizeRidesPool = (size) => {
    if (ridesPool.length === size) return false;
    if (size < ridesPool.length) {
      ridesPool.length = size;
    } else {
      while (ridesPool.length < size) ridesPool.push(null);
    }
    return true;
  };
  // Cached inputs used by the rides layer. These are refreshed by the
  // main buildLayers() pass and re-read by the standalone rides rAF, so
  // the animation loop can produce fresh frames without re-running the
  // entire interaction-derivation pipeline (which is what was breaking
  // hover/click responsiveness).
  let ridesCtx = {
    posLookup: {},
    palRgb: [],
    stationCluster: new Map(),
    spatialMix: 0,
    focus: null,
    focusCtx: null,
    available: false,
  };

  const ensureSampler = (transitionTopk, stationOutflow) => {
    const tkKeys = transitionTopk ? Object.keys(transitionTopk) : [];
    const ofKeys = stationOutflow ? Object.keys(stationOutflow) : [];
    const sig = `${tkKeys.length}:${tkKeys[0] || ''}|${ofKeys.length}:${ofKeys[0] || ''}`;
    if (sig === lastSamplerSig) return false;
    ridesSampler = makeRideSampler(transitionTopk || {}, stationOutflow || {});
    lastSamplerSig = sig;
    for (let i = 0; i < ridesPool.length; i += 1) ridesPool[i] = null;
    return true;
  };

  const flushRidesForSelectionChange = (kind, focusName, focusCtx) => {
    // Build a stable signature describing "what mode the rides are in
    // right now". Anything that changes the spawn distribution (kind,
    // focused station, mat-cell pair, selection set) bumps the sig.
    let sig = kind || 'ambient';
    if (focusCtx) {
      if (focusCtx.mode === 'station') sig += `|${focusCtx.focus || ''}`;
      else if (focusCtx.mode === 'mat_cell') sig += `|${focusCtx.from}->${focusCtx.to}`;
      else if (focusCtx.mode === 'col_dendro') {
        sig += `|out:${focusCtx.origin.names.slice().sort().join(',')}`;
      } else if (focusCtx.mode === 'row_dendro') {
        sig += `|in:${focusCtx.dest.names.slice().sort().join(',')}`;
      }
    } else if (focusName) {
      sig += `|${focusName}`;
    }
    if (sig === lastFocusForRides) return false;
    lastFocusForRides = sig;
    for (let i = 0; i < ridesPool.length; i += 1) ridesPool[i] = null;
    return true;
  };

  /** Build the bike-rides ScatterplotLayer using the cached `ridesCtx`. */
  const makeRidesLayer = () => {
    if (!ridesCtx.available) return null;
    const { posLookup, palRgb, stationCluster, spatialMix, focus, focusCtx } = ridesCtx;
    const now = performance.now();
    const dtMs = lastRidesFrameTs > 0
      ? Math.max(0, Math.min(120, now - lastRidesFrameTs))
      : 16;
    lastRidesFrameTs = now;
    advanceRides(ridesPool, dtMs, ridesSampler, posLookup, palRgb, stationCluster, focusCtx);
    refillRides(ridesPool, ridesSampler, focus || null, posLookup, palRgb, stationCluster, focusCtx);
    ridesFrame += 1;
    const ridesAlpha = Math.round(220 * Math.max(0, 1 - spatialMix));
    // Dendrogram & manual-category selections paint rides in the
    // selected group's cluster colors, which can be very light (yellow,
    // mint, etc.) and hard to pick out against the basemap. Add a thin
    // dark-grey stroke in those modes so each dot reads clearly.
    const RIDES_STROKE_MODES = new Set(['col_dendro', 'row_dendro']);
    const stroked = Boolean(focusCtx && RIDES_STROKE_MODES.has(focusCtx.mode));
    const strokeAlpha = Math.round(180 * Math.max(0, 1 - spatialMix));
    return new ScatterplotLayer({
      id: 'bike-rides',
      data: ridesPool,
      pickable: false,
      radiusUnits: 'pixels',
      radiusMinPixels: 0.8,
      radiusMaxPixels: 2.2,
      getRadius: 1.3,
      getPosition: (d) => (d ? d.position : [0, 0]),
      getFillColor: (d) => {
        if (!d) return [0, 0, 0, 0];
        const c = d.color;
        return [c[0], c[1], c[2], ridesAlpha];
      },
      stroked,
      lineWidthUnits: 'pixels',
      lineWidthMinPixels: stroked ? 0.6 : 0,
      getLineWidth: stroked ? 0.8 : 0,
      getLineColor: [55, 60, 70, strokeAlpha],
      // Position changes every frame; bump trigger every frame so deck.gl
      // re-runs the accessor. Other accessors only invalidate on hops,
      // focus/sampler swaps, or palette changes.
      updateTriggers: {
        getPosition: ridesFrame,
        getFillColor: [ridesFrame, palRgb, ridesAlpha],
        getLineColor: [stroked, strokeAlpha],
        getLineWidth: stroked,
      },
    });
  };

  // Cache of the most recent non-rides layer stack so the rides rAF can
  // splice in a fresh rides layer without rebuilding everything else
  // (which is what was destroying hover/click interactivity at 60fps).
  let cachedNonRidesLayers = [];

  const buildLayers = () => {
    const stations = store.stations.get() || [];
    const focus = store.focus.get();
    const highlightArr = store.highlights.get() || [];
    const highlights = new Set(highlightArr);
    const edges = store.edges.get() || [];
    const stationNameSet = new Set(stations.map((d) => d.name));
    const overlap = highlightArr.filter((n) => stationNameSet.has(n));
    log(store, 'buildLayers', {
      focus,
      highlightCount: highlightArr.length,
      overlap: overlap.length,
      sample: overlap.slice(0, 5),
      stations: stations.length,
      edges: edges.length,
      version: renderVersion,
    });
    const hasSel = Boolean(focus) || highlights.size > 0;
    const spatialMix = store.spatial_mix.get();
    const hoveredCluster = store.hovered_cluster.get();
    const pinnedCluster = store.pinned_cluster.get();
    // Set of cluster ids whose neighborhood should be visually emphasized
    // (highlighted polygon + station-dim). Priority:
    //   1. pinned cluster (click-pinned NBHD, persists across renders),
    //   2. direct station / NBHD hover (`hovered_cluster` = single id),
    //   3. active `highlights` selection (cat_value, mat_value cell linking
    //      two stations, dendrogram pick, ...). Multiple clusters can be
    //      active at once when e.g. a matrix cell links two clusters.
    const derivedClusters = new Set();
    if (pinnedCluster != null && pinnedCluster !== 0) {
      derivedClusters.add(pinnedCluster);
    }
    if (hoveredCluster != null && hoveredCluster !== 0) {
      derivedClusters.add(hoveredCluster);
    }
    if (derivedClusters.size === 0 && highlights.size > 0) {
      const stationCluster = new Map();
      for (const s of stations) stationCluster.set(s.name, s.cluster_id);
      for (const n of highlights) {
        const cid = stationCluster.get(n);
        if (cid != null && cid !== 0) derivedClusters.add(cid);
      }
    }
    const hasDerived = derivedClusters.size > 0;
    // Station-dim logic uses a single cluster id. Pin wins when nothing else
    // is hovered/focused, hover trumps pin while the cursor is on the map.
    const stationFocusCluster = hoveredCluster != null
      ? hoveredCluster
      : (pinnedCluster != null ? pinnedCluster : null);
    const posLookup = {};
    let geoSumLng = 0, geoSumLat = 0, umapSumLng = 0, umapSumLat = 0, umapN = 0;
    for (const s of stations) {
      geoSumLng += Number(s.lng); geoSumLat += Number(s.lat);
      if (s.umap_lng != null && s.umap_lat != null) {
        umapSumLng += Number(s.umap_lng); umapSumLat += Number(s.umap_lat); umapN += 1;
      }
    }
    const n = stations.length || 1;
    const geoCenterLng = geoSumLng / n, geoCenterLat = geoSumLat / n;
    const umapCenterLng = umapN ? umapSumLng / umapN : geoCenterLng;
    const umapCenterLat = umapN ? umapSumLat / umapN : geoCenterLat;
    const dLng = geoCenterLng - umapCenterLng, dLat = geoCenterLat - umapCenterLat;
    for (const s of stations) {
      const t = spatialMix;
      const hasUmap = s.umap_lng != null && s.umap_lat != null;
      const uLng = hasUmap ? Number(s.umap_lng) + dLng : Number(s.lng);
      const uLat = hasUmap ? Number(s.umap_lat) + dLat : Number(s.lat);
      const lng = Number(s.lng) * (1 - t) + uLng * t;
      const lat = Number(s.lat) * (1 - t) + uLat * t;
      posLookup[s.name] = [lng, lat];
    }
    const { out, inn } = getLinkedWeights();
    const highlightKey = Array.from(highlights).sort().join('|');
    const outKey = Array.from(out.entries())
      .map(([k, v]) => `${k}:${v}`)
      .sort()
      .join('|');
    const inKey = Array.from(inn.entries())
      .map(([k, v]) => `${k}:${v}`)
      .sort()
      .join('|');
    const styleKey = `${focus}__${highlightKey}__${outKey}__${inKey}__${hasSel}__${spatialMix}__${hoveredCluster}__${pinnedCluster}`;

    let peakLinkWeight = 0;
    for (const v of out.values()) peakLinkWeight = Math.max(peakLinkWeight, v);
    for (const v of inn.values()) peakLinkWeight = Math.max(peakLinkWeight, v);

    let hubSumOut = 0;
    let hubSumIn = 0;
    for (const v of out.values()) hubSumOut += Number(v) || 0;
    for (const v of inn.values()) hubSumIn += Number(v) || 0;

    const linkedRadius = (n) => {
      const wo = out.has(n) ? Math.min(1, Math.max(0, out.get(n) || 0)) : 0;
      const wi = inn.has(n) ? Math.min(1, Math.max(0, inn.get(n) || 0)) : 0;
      const w = Math.max(wo, wi);
      return 78 + 96 * Math.sqrt(w);
    };

    const focusHubRadius = () =>
      138 + 38 * Math.sqrt(Math.min(1, peakLinkWeight));

    const palRgb = resolvePaletteRgb(store);

    // ---- Alpha-shape neighborhood polygons (rendered below stations) ----
    // We only morph vertices when spatial_mix > 0 to keep the common case fast.
    const cp = store.cluster_polygons.get() || {};
    const showNbhd = store.show_neighborhoods.get();
    const alphaIdx = Math.max(0, Math.min(
      (cp.levels_miles?.length || 1) - 1,
      Number(store.alpha_index.get()) | 0,
    ));
    // polyData: one entry per (cluster, polygon-part). Vertices morph between
    // geo and UMAP space when the spatial slider moves, but we also fade the
    // whole layer toward 0 alpha as we approach UMAP — alpha shapes computed
    // in geographic space don't carry semantic meaning once they're warped
    // into the UMAP layout.
    const polyData = [];
    if (showNbhd && Array.isArray(cp.polygons) && cp.polygons.length) {
      const t = spatialMix;
      const useUmap = t > 0;
      for (const cluster of cp.polygons) {
        const byLevel = cluster.by_level || [];
        const polys = byLevel[alphaIdx] || [];
        for (let pi = 0; pi < polys.length; pi += 1) {
          const p = polys[pi];
          const geoRings = p.geo || [];
          const umapRings = p.umap || [];
          // deck.gl PolygonLayer accepts [outer, hole1, hole2, ...]; each ring [[lng,lat],...]
          let rings;
          if (!useUmap) {
            rings = geoRings;
          } else {
            rings = geoRings.map((ring, ri) => {
              const uring = umapRings[ri] || ring;
              return ring.map(([lng, lat], vi) => {
                const u = uring[vi] || [lng, lat];
                const ulng = Number(u[0]) + dLng;
                const ulat = Number(u[1]) + dLat;
                return [Number(lng) * (1 - t) + ulng * t, Number(lat) * (1 - t) + ulat * t];
              });
            });
          }
          polyData.push({
            cluster_id: cluster.cluster_id,
            poly_id: pi,
            polygon: rings,
          });
        }
      }
    }

    // Stable cache key for `derivedClusters` so deck.gl can detect changes via
    // updateTriggers (Set identity isn't enough — we need a value-based key).
    const derivedKey = [...derivedClusters].sort((a, b) => a - b).join(',');
    const isDerived = (cid) => derivedClusters.has(cid);
    // Linear fade as we morph toward UMAP — geo alpha shapes lose meaning once
    // they're warped, so by spatial_mix=1 the layer is fully invisible. The
    // user-controlled nbhd_opacity_mult (0..1) is folded into the same scalar
    // so the slider acts on both fill and stroke uniformly: 0 = fully
    // transparent, 1 = full opacity, default 0.4 = subtle.
    const nbhdOpacityMult = Math.max(0, Math.min(1, Number(store.nbhd_opacity_mult.get()) || 0));
    const nbhdFade = Math.max(0, 1 - spatialMix) * nbhdOpacityMult;
    // Highlight pushes the *border* (width + alpha) rather than the fill, so
    // selected NBHDs read as outlined regions and the stations inside stay
    // legible. Alpha values are chosen so that at the slider's maximum
    // (nbhdOpacityMult=1.0) the idle fill and hover border both saturate at
    // 255 (fully opaque). At the default 0.4 they sit at ~40% — a usable
    // mid-range. Hover state stays differentiated from idle primarily via
    // line width (3.2px vs 0.8px) once fill saturates.
    const NBHD_FILL_IDLE = 255;
    const NBHD_FILL_HOVER = 255;
    const NBHD_FILL_DIM = 60;
    const NBHD_LINE_IDLE_ALPHA = 200;
    const NBHD_LINE_HOVER_ALPHA = 255;
    const NBHD_LINE_DIM_ALPHA = 80;
    const NBHD_LINE_IDLE_W = 0.8;
    const NBHD_LINE_HOVER_W = 3.2;
    const NBHD_LINE_DIM_W = 0.4;
    // Including alphaIdx in the layer id means a resolution change replaces
    // the layer wholesale instead of streaming new polygon buffers into the
    // existing one. Without this, deck.gl tries to interpolate attribute
    // transitions across mismatched vertex counts and you see a flicker on
    // every slider step.
    const polygons = new PolygonLayer({
      id: `bike-cluster-nbhd-${alphaIdx}`,
      data: polyData,
      visible: showNbhd && polyData.length > 0 && nbhdFade > 0.001,
      // Pick while the layer is meaningfully present: not faded into UMAP
      // and not dialed to (near-)transparent. The opacity threshold is
      // intentionally low so hover still works at the default 0.4.
      pickable: (1 - spatialMix) > 0.5 && nbhdOpacityMult > 0.05,
      stroked: true,
      filled: true,
      lineWidthUnits: 'pixels',
      getPolygon: (d) => d.polygon,
      getFillColor: (d) => {
        const [r, g, b] = clusterRgbFromId(d.cluster_id, palRgb);
        const a = !hasDerived
          ? NBHD_FILL_IDLE
          : isDerived(d.cluster_id) ? NBHD_FILL_HOVER : NBHD_FILL_DIM;
        return [r, g, b, Math.round(a * nbhdFade)];
      },
      getLineColor: (d) => {
        const [r, g, b] = clusterRgbFromId(d.cluster_id, palRgb);
        const a = !hasDerived
          ? NBHD_LINE_IDLE_ALPHA
          : isDerived(d.cluster_id) ? NBHD_LINE_HOVER_ALPHA : NBHD_LINE_DIM_ALPHA;
        return [r, g, b, Math.round(a * nbhdFade)];
      },
      getLineWidth: (d) => {
        const w = !hasDerived
          ? NBHD_LINE_IDLE_W
          : isDerived(d.cluster_id) ? NBHD_LINE_HOVER_W : NBHD_LINE_DIM_W;
        return w * nbhdFade;
      },
      transitions: {
        getFillColor: 250,
        getLineColor: 250,
        getLineWidth: 250,
      },
      updateTriggers: {
        // alphaIdx is not here — the layer id already changes with it, so
        // deck.gl will instantiate a fresh layer rather than retriggering
        // attribute updates against stale geometry.
        getFillColor: [derivedKey, palRgb, spatialMix, nbhdOpacityMult, renderVersion],
        getLineColor: [derivedKey, palRgb, spatialMix, nbhdOpacityMult, renderVersion],
        getLineWidth: [derivedKey, spatialMix, nbhdOpacityMult, renderVersion],
        getPolygon: [spatialMix, renderVersion],
      },
      onHover: (info) => {
        const cid = info.object ? info.object.cluster_id : null;
        if (store.hovered_cluster.get() !== cid) {
          store.hovered_cluster.set(cid);
          scheduleRender();
        }
      },
      onClick: (info) => {
        if (!info.object) return;
        // NBHD click pins the cluster for a persistent, discussion-ready
        // highlight. Clicking the same cluster again unpins. We also clear any
        // prior clustergram-driven selection (focus/highlights/edges +
        // click_info / matrix_axis_slice upstream) so the pinned NBHD view
        // starts clean.
        const cid = Number(info.object.cluster_id);
        const current = store.pinned_cluster.get();
        const next = current === cid ? null : cid;
        store.pinned_cluster.set(next);
        const hadSel = Boolean(store.focus.get())
          || (store.highlights.get() || []).length > 0
          || (store.edges.get() || []).length > 0;
        if (hadSel) {
          setDerivedState(store, { focus: '', highlights: [], edges: [] });
          lastActionKey = null;
          model.set('click_info', {});
          model.set('matrix_axis_slice', {});
          model.save_changes();
        }
        scheduleRender();
        log(store, 'nbhd click -> pin', next);
      },
    });

    // Force a new data container each render so style-only updates always propagate
    const pointData = stations.map((d) => d);

    const showStations = store.show_stations.get();
    const stationSizeMult = Math.max(0.05, Number(store.station_size_mult.get()) || 1);
    // Dark-gray outline for emphasized stations (focus hub, focus neighbors,
    // category-highlighted, hover/pin cluster). The outline sits over the
    // fill, so it reads cleanly against any cluster color or basemap shade —
    // the main reason it exists is to make small dots discoverable at low zoom
    // without having to crank the radius.
    const STATION_STROKE = [44, 50, 60];
    const isStationEmphasized = (d) => {
      const n = d.name;
      if (focus) return n === focus || out.has(n) || inn.has(n);
      if (highlights.size > 0) return highlights.has(n);
      if (stationFocusCluster != null) return d.cluster_id === stationFocusCluster;
      return false;
    };
    const points = new ScatterplotLayer({
      id: 'bike-stations',
      data: pointData,
      visible: showStations,
      pickable: showStations,
      radiusUnits: 'meters',
      // Low-ish min-pixel floor so dots still shrink at low zooms;
      // selected/focus stations stay readable via their larger meter radii.
      radiusMinPixels: 1.5 * stationSizeMult,
      radiusMaxPixels: 48 * stationSizeMult,
      stroked: true,
      lineWidthUnits: 'pixels',
      lineWidthMinPixels: 0,
      getPosition: (d) => posLookup[d.name] || [Number(d.lng), Number(d.lat)],
      getRadius: (d) => {
        const n = d.name;
        let r;
        if (focus && n === focus) r = focusHubRadius();
        else if (focus && (out.has(n) || inn.has(n))) r = linkedRadius(n);
        else if (highlights.has(n)) r = 66;
        else r = hasSel ? 50 : 66;
        return r * stationSizeMult;
      },
      getLineColor: (d) =>
        isStationEmphasized(d) ? [...STATION_STROKE, 220] : [0, 0, 0, 0],
      getLineWidth: (d) => (isStationEmphasized(d) ? 1.5 : 0),
      getFillColor: (d) => {
        const n = d.name;
        // Focus / traffic: hub + neighbors use red↔blue blend from out vs in weight (lines still encode direction/width).
        if (focus) {
          if (n === focus) {
            return flowBlendWithAlpha(hubSumOut, hubSumIn, 248);
          }
          if (out.has(n) || inn.has(n)) {
            const wo = out.has(n) ? Math.max(0, out.get(n) || 0) : 0;
            const wi = inn.has(n) ? Math.max(0, inn.get(n) || 0) : 0;
            const w = Math.max(wo, wi);
            const a = Math.round(148 + 102 * Math.sqrt(Math.min(1, w)));
            return flowBlendWithAlpha(wo, wi, a);
          }
          return TRAFFIC_UNCONNECTED_RGBA;
        }
        if (highlights.size > 0) {
          const c = clusterFillColor(d.cluster_id, palRgb);
          if (highlights.has(n)) return [c[0], c[1], c[2], 242];
          return [c[0], c[1], c[2], 36];
        }
        if (stationFocusCluster != null) {
          if (d.cluster_id === stationFocusCluster) return clusterFillWithAlpha(d.cluster_id, 248, palRgb);
          return clusterFillWithAlpha(d.cluster_id, 50, palRgb);
        }
        return clusterFillColor(d.cluster_id, palRgb);
      },
      transitions: {
        getFillColor: 400,
        getRadius: 400,
        getLineColor: 250,
        getLineWidth: 250,
      },
      updateTriggers: {
        getRadius: [styleKey, stationSizeMult, renderVersion],
        getFillColor: [styleKey, renderVersion],
        getLineColor: [styleKey, renderVersion],
        getLineWidth: [styleKey, renderVersion],
        getPosition: [spatialMix, renderVersion],
      },
      onHover: (info) => {
        const cid = info.object ? info.object.cluster_id : null;
        if (store.hovered_cluster.get() !== cid) {
          store.hovered_cluster.set(cid);
          scheduleRender();
        }
      },
      onClick: (info) => {
        if (!info.object) return;
        const n = String(info.object.name || '').trim();
        // Station click always exits NBHD-pin mode — it's a distinct focus
        // gesture. Hover state clears naturally via the render pipeline.
        if (store.pinned_cluster.get() != null) store.pinned_cluster.set(null);
        if (store.focus.get() === n) {
          setDerivedState(store, { focus: '', highlights: [], edges: [] });
          lastActionKey = null;
          model.set('click_info', {});
          model.set('matrix_axis_slice', {});
          model.save_changes();
          scheduleRender();
          log(store, 'map toggle off', n);
          return;
        }
        const rowNames = model.get('cg_row_names') || [];
        const colNames = model.get('cg_col_names') || [];
        const rowIx = findAxisIndex(rowNames, n);
        const colIx = findAxisIndex(colNames, n);
        if (rowIx < 0 || colIx < 0) {
          log(store, 'map click: missing matrix axis index', n, 'row', rowIx, 'col', colIx);
          return;
        }
        model.set('click_info', {});
        model.set('matrix_axis_slice', {});
        model.save_changes();
        pushRowColMatrixSliceRequest(model, rowIx, colIx);
        setDerivedState(store, { focus: n, highlights: [n], edges: [] });
        lastActionKey = null;
        scheduleRender();
        log(store, 'map click -> row_col request', n, rowIx, colIx);
      },
    });

    // geom_share in [0,1]; scale like matrix emphasis (stronger than before, pixel units).
    const lineWidthFor = (d) => {
      if (d.direction === 'direct') {
        const g = Number(d.geom_share);
        const x = Number.isFinite(g) ? Math.min(1, Math.max(0, g)) : 0;
        return 4 + 14 * x;
      }
      const g = Number(d.geom_share);
      if (Number.isFinite(g) && g > 0) {
        const x = Math.min(1, g);
        return 4 + 22 * x ** 0.92;
      }
      return 2.5;
    };

    const lines = new LineLayer({
      id: 'bike-flow-lines',
      data: edges,
      widthUnits: 'pixels',
      getSourcePosition: (d) => posLookup[d.source_name] || d.source,
      getTargetPosition: (d) => posLookup[d.target_name] || d.target,
      getWidth: lineWidthFor,
      updateTriggers: {
        getWidth: [styleKey, renderVersion],
        getSourcePosition: [spatialMix, renderVersion],
        getTargetPosition: [spatialMix, renderVersion],
      },
      getColor: (d) => {
        const a = Math.round(Math.max(0, Math.min(1, Number(d.opacity) || 0)) * 255);
        if (d.direction === 'in') return [70, 150, 255, a];
        if (d.direction === 'out') return [255, 70, 70, a];
        // 'direct' = matrix-cell click linking two stations. Use a near-black
        // line so it reads against both the map and the highlighted NBHDs;
        // bump its minimum opacity so even small probabilities stay visible.
        return [24, 28, 36, Math.max(220, a)];
      },
    });

    // ---- Refresh ride simulator inputs ------------------------------
    // We do NOT build the rides layer here every time — that would force
    // every state-change render (sliders, hover, etc.) to also rebuild
    // 1000 walkers. Instead we cache the inputs the rides rAF needs and
    // let it produce its own layer. The rAF splices the rides layer
    // into the deck via setProps, alongside the cached non-rides layers
    // we save below. This is what restored hover/click interactivity.
    const showRides = !!store.show_rides.get();
    const transitionTopk = store.transition_topk.get() || {};
    const stationOutflow = store.station_outflow.get() || {};
    if (showRides && Object.keys(transitionTopk).length > 0) {
      const palRgb = resolvePaletteRgb(store);
      const stationCluster = new Map();
      for (const s of stations) stationCluster.set(s.name, Number(s.cluster_id) || 0);
      // Selection drives both the ride-context shape (single-station
      // in/out, mat cell pair, dendro/cat origin or destination set)
      // and the pool size — focused views scale to ~10% so the geometry
      // stays legible.
      const selectionKind = store.selection_kind.get();
      const focusCtx = selectionKind
        ? buildFocusedRideContext({
          kind: selectionKind,
          focus,
          highlights: highlightArr,
          edges,
          transitionTopk,
          stationOutflow,
          posLookup,
        })
        : null;
      const isFocused = Boolean(focusCtx);
      // Pool resize: ambient = slider value. Narrow selections (single
      // station / matrix cell) shrink to ~10%. Dendro / category
      // selections scale by selection_size / total_stations so the
      // per-station ride density stays constant regardless of cluster
      // size — pick a small cluster, get a small swarm.
      const nRidesSlider = Number(store.n_rides.get());
      resizeRidesPool(
        targetRidesPoolSize(nRidesSlider, selectionKind, focusCtx, stations.length),
      );
      ensureSampler(transitionTopk, stationOutflow);
      // Flush the pool whenever the selection kind/identity changes so
      // the swarm switches cleanly between modes (no stale rides from
      // a prior focus continuing under the new one).
      flushRidesForSelectionChange(selectionKind, focus, focusCtx);
      ridesCtx = {
        posLookup,
        palRgb,
        stationCluster,
        spatialMix,
        focus: focus || null,
        focusCtx,
        available: true,
      };
    } else {
      ridesCtx = { ...ridesCtx, available: false };
      lastRidesFrameTs = 0;
    }

    const basemapAlpha = Math.round(255 * (1 - spatialMix));
    // Non-rides layers in render order. We cache this so the rides rAF
    // can append a fresh rides layer without redoing the work above.
    const nonRides = [basemapLayer(basemapAlpha), polygons, lines, points];
    cachedNonRidesLayers = nonRides;
    if (ridesCtx.available) {
      const ridesLayer = makeRidesLayer();
      if (ridesLayer) {
        // Insert just below the station points so dots overlay cluster
        // colors but don't shadow station picking.
        return [...nonRides.slice(0, -1), ridesLayer, nonRides[nonRides.length - 1]];
      }
    }
    return nonRides;
  };

  let pendingProps = null;

  const prepareDeckProps = () => {
    const stations = store.stations.get() || [];
    const w = Number(store.width.get() || 560);
    const h = Number(store.height.get() || 800);
    root.style.width = `${w}px`;
    root.style.height = `${h}px`;
    // Canvas takes whatever's left after the toolbar; clamp to a sane minimum
    // so very-short widgets still render *something*.
    const canvasH = Math.max(120, h - TOPBAR_HEIGHT);
    mapHolder.style.width = `${w}px`;
    mapHolder.style.height = `${canvasH}px`;

    const props = {
      parent: mapHolder,
      width: w,
      height: canvasH,
      controller: { doubleClickZoom: false },
      layers: buildLayers(),
      getTooltip: ({ object, layer }) => {
        if (!object || layer.id !== 'bike-stations') return null;
        const nm = object.name != null ? String(object.name) : '';
        const cid = object.cluster_id != null ? String(object.cluster_id) : '';
        const clusterLine =
          cid && cid !== '0'
            ? `<br/><span style="color:#666;">Cluster ${cid}</span>`
            : '';
        const html = `<div style="font:12px system-ui,sans-serif;"><b>${nm}</b>${clusterLine}</div>`;
        return {
          html,
          style: {
            backgroundColor: 'rgba(255,255,255,0.94)',
            color: '#1a1d24',
            padding: '6px 10px',
            borderRadius: '4px',
            boxShadow: '0 1px 4px rgba(0,0,0,0.18)',
          },
        };
      },
    };

    if (!deck) {
      deck = new Deck({ ...props, initialViewState: fitViewState(stations) });
      log(store, 'deck init', 'stations', stations.length);
      pendingProps = null;
      return;
    }

    pendingProps = props;
  };

  const applyDeckProps = () => {
    if (deck && pendingProps) {
      deck.setProps(pendingProps);
      if (typeof deck.redraw === 'function') deck.redraw(true);
      log(store, 'deck setProps', {
        layers: pendingProps.layers.length,
        highlights: (store.highlights.get() || []).length,
        edges: (store.edges.get() || []).length,
        version: renderVersion,
      });
      pendingProps = null;
    }
  };

  store.deck_check.subscribe(
    (check) => {
      const ready = Object.values(check).every((v) => v === true);
      if (store.deck_ready.get() !== ready) store.deck_ready.set(ready);
    },
    { immediate: true }
  );

  store.deck_ready.subscribe(
    (ready) => {
      if (!ready) return;
      log(store, 'deck_ready -> apply');
      applyDeckProps();
    },
    { immediate: false }
  );

  const scheduleRender = () => {
    if (raf) return;
    raf = requestAnimationFrame(() => {
      raf = 0;
      renderVersion += 1;
      log(store, 'flush start', { version: renderVersion });
      store.deck_check.set({ inputs: false, computed: false, layers: false });

      // 1) inputs considered synced for this frame
      store.deck_check.set({ ...store.deck_check.get(), inputs: true });

      // 2) derive interaction state
      computeStateFromInputs();
      store.deck_check.set({ ...store.deck_check.get(), computed: true });

      // 3) prepare deck props (or init deck)
      prepareDeckProps();
      store.deck_check.set({ ...store.deck_check.get(), layers: true });

      log(store, 'flush end', store.deck_check.get());
    });
  };

  // subscriptions: any change schedules one render pass
  [
    store.stations,
    store.edge_index,
    store.click_info,
    store.selected_rows,
    store.selected_cols,
    store.width,
    store.height,
    store.palette_rgb,
    store.matrix_axis_slice,
    store.spatial_mix,
    store.cluster_polygons,
    store.alpha_index,
    store.show_neighborhoods,
    store.show_stations,
    store.pinned_cluster,
    store.station_size_mult,
    store.nbhd_opacity_mult,
    store.transition_topk,
    store.station_outflow,
    store.show_rides,
    store.n_rides,
    store.selection_kind,
  ].forEach((obs) => obs.subscribe(() => scheduleRender(), { immediate: false }));

  // ---- Rides animation loop ------------------------------------------
  // Standalone rAF that only swaps the bike-rides layer in; all other
  // layers come from the cached `cachedNonRidesLayers`. This bypasses
  // computeStateFromInputs / prepareDeckProps / deck.redraw entirely
  // each frame, which is what made hover/click feel jammed when rides
  // were on (the main pipeline was being re-run 60×/sec).
  let ridesRaf = 0;
  const ridesTick = () => {
    ridesRaf = 0;
    if (!store.show_rides.get()) return;
    if (!ridesCtx.available) return;
    if (!deck) return;
    const ridesLayer = makeRidesLayer();
    if (ridesLayer && cachedNonRidesLayers.length) {
      // Splice rides just below the station points (last cached layer)
      // so dots overlay cluster colors but don't shadow station picking.
      const layers = [
        ...cachedNonRidesLayers.slice(0, -1),
        ridesLayer,
        cachedNonRidesLayers[cachedNonRidesLayers.length - 1],
      ];
      deck.setProps({ layers });
    }
    ridesRaf = requestAnimationFrame(ridesTick);
  };
  const ensureRidesAnimating = () => {
    if (ridesRaf) return;
    if (!store.show_rides.get()) return;
    if (!Object.keys(store.transition_topk.get() || {}).length) return;
    ridesRaf = requestAnimationFrame(ridesTick);
  };
  store.show_rides.subscribe(() => ensureRidesAnimating(), { immediate: false });
  store.transition_topk.subscribe(() => ensureRidesAnimating(), { immediate: false });

  const syncFromModel = () => {
    const ci = JSON.stringify(model.get('click_info') || {});
    const rows = JSON.stringify(model.get('selected_rows') || []);
    const cols = JSON.stringify(model.get('selected_cols') || []);
    const sl = JSON.stringify(model.get('matrix_axis_slice') || {});
    if (ci !== linkSnap.ci || rows !== linkSnap.rows || cols !== linkSnap.cols || sl !== linkSnap.sl) {
      linkSnap = { ci, rows, cols, sl };
    }
    log(store, 'syncFromModel start');
    store.stations.set(model.get('stations') || []);
    store.palette_rgb.set(model.get('palette_rgb') || []);
    store.edge_index.set(model.get('edge_index') || {});
    store.click_info.set(model.get('click_info') || {});
    store.selected_rows.set(model.get('selected_rows') || []);
    store.selected_cols.set(model.get('selected_cols') || []);
    store.matrix_axis_slice.set(model.get('matrix_axis_slice') || {});
    store.width.set(model.get('width') || 560);
    store.height.set(model.get('height') || 800);
    store.debug.set(Boolean(model.get('debug')));
    const mixVal = model.get('spatial_mix') || 0;
    store.spatial_mix.set(mixVal);
    slider.value = String(mixVal);
    const cp = model.get('cluster_polygons') || {};
    store.cluster_polygons.set(cp && typeof cp === 'object' ? cp : {});
    const ai = Number(model.get('alpha_index'));
    store.alpha_index.set(Number.isFinite(ai) ? (ai | 0) : 4);
    store.show_neighborhoods.set(Boolean(model.get('show_neighborhoods') ?? true));
    store.show_stations.set(Boolean(model.get('show_stations') ?? true));
    store.show_rides.set(Boolean(model.get('show_rides') ?? true));
    const nr = Number(model.get('n_rides'));
    store.n_rides.set(
      Number.isFinite(nr)
        ? Math.max(N_RIDES_MIN, Math.min(N_RIDES_MAX, nr | 0))
        : N_RIDES_DEFAULT,
    );
    const tk = model.get('transition_topk') || {};
    store.transition_topk.set(tk && typeof tk === 'object' ? tk : {});
    const so = model.get('station_outflow') || {};
    store.station_outflow.set(so && typeof so === 'object' ? so : {});
    log(store, 'syncFromModel done', {
      linkSeq: linkInteractionSeq,
      rows: (store.selected_rows.get() || []).length,
      cols: (store.selected_cols.get() || []).length,
      clickType: (store.click_info.get() || {}).type || null,
    });
    scheduleRender();
  };

  model.on('change:click_info', () => {
    linkInteractionSeq += 1;
    syncFromModel();
  });

  [
    'stations',
    'edge_index',
    'selected_rows',
    'selected_cols',
    'width',
    'height',
    'debug',
    'palette_rgb',
    'matrix_axis_slice',
    'spatial_mix',
    'cluster_polygons',
    'alpha_index',
    'show_neighborhoods',
    'show_stations',
    'show_rides',
    'n_rides',
    'transition_topk',
    'station_outflow',
  ].forEach((name) => model.on(`change:${name}`, syncFromModel));

  model.on('change:cg_row_names', () => scheduleRender());
  model.on('change:cg_col_names', () => scheduleRender());

  syncFromModel();

  return () => {
    clearTimeout(hoverTimer);
    if (raf) cancelAnimationFrame(raf);
    if (ridesRaf) cancelAnimationFrame(ridesRaf);
    if (deck) deck.finalize();
  };
}

export default { render };
