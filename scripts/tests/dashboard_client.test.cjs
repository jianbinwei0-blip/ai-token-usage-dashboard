// Exercise the real inline dashboard program without dependencies or live data.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const template = fs.readFileSync(path.join(__dirname, '../../dashboard/index.html'), 'utf8');
const program = template.slice(template.lastIndexOf('<script>') + '<script>'.length, template.lastIndexOf('</script>'));

class Element {
  constructor() { this.value = ''; this.textContent = ''; this.innerHTML = ''; this.children = []; this.style = {}; this.listeners = {}; this.classList = {add() {}, toggle() {}}; }
  addEventListener(type, fn) { this.listeners[type] = fn; }
  appendChild(child) { this.children.push(child); return child; }
  append(...children) { this.children.push(...children); }
  setAttribute(name, value) { this[name] = value; }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  focus() {}
}
const storage = (initial = {}) => {
  const values = new Map(Object.entries(initial));
  return {getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, String(value)), removeItem: key => values.delete(key)};
};
function dashboard({dataset = {}, now = '2026-11-03T12:00:00', fetch = async () => {throw new Error('offline');}, saved = {}} = {}) {
  const elements = new Map();
  for (const match of template.matchAll(/\bid="([^"]+)"/g)) elements.set(match[1], new Element());
  elements.get('usageDataset').textContent = JSON.stringify(dataset);
  elements.get('usageChartSort').value = 'tokens-desc';
  const timers = new Map(); let serial = 0; let reloads = 0;
  class Clock extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return new Date(now).getTime(); } }
  const document = {getElementById: id => elements.get(id) ?? null, querySelectorAll: () => [], createElement: () => new Element(), visibilityState: 'visible', addEventListener() {}};
  const window = {setTimeout: (fn, delay) => {const id = ++serial; timers.set(id, {fn, delay}); return id;}, clearTimeout: id => timers.delete(id), location: {reload: () => reloads++}, addEventListener() {}};
  const context = vm.createContext({document, window, localStorage: storage(saved), sessionStorage: storage({'aiTokenDashboardAutoRecalcPending': '1'}), fetch, AbortController, Date: Clock, console, Intl});
  vm.runInContext(program, context);
  return {elements, timers, context, evaluate: expr => vm.runInContext(expr, context), get reloads() { return reloads; }, async tick() {await new Promise(resolve => setImmediate(resolve));}};
}
const row = (date, total = 110, model = 'gpt-6-astra') => ({date, sessions: 1, input_tokens: total - 10, output_tokens: 10, cached_tokens: 0, total_tokens: total, total_cost_usd: 0.0015, cost_complete: true, breakdown_rows: [{agent_cli: 'codex', model, sessions: 1, total_tokens: total, total_cost_usd: 0.0015, cost_complete: true}]});

test('a failed scheduled refresh preserves the snapshot and schedules recovery', async () => {
  const app = dashboard({dataset: {providers: {codex: {rows: [row('2026-11-03')]}}}});
  const before = app.elements.get('dailyUsageTableBody').innerHTML;
  const [id, timer] = [...app.timers.entries()][0]; app.timers.delete(id); timer.fn(); await app.tick();
  assert.equal(app.reloads, 0);
  assert.equal(app.elements.get('dailyUsageTableBody').innerHTML, before);
  assert.match(app.elements.get('autoRecalcStatus').textContent, /stale|retry/i);
  assert.ok(app.timers.size > 0, 'a transient failure must not end automatic refresh');
  assert.equal(app.elements.get('retryRecalc').hidden, false);
});

test('calendar presets include an empty current period instead of prior-month activity', () => {
  const app = dashboard({dataset: {providers: {codex: {rows: [row('2026-10-31')]}}}});
  app.evaluate('applyPresetRange("mtd")');
  assert.equal(app.elements.get('usageRangeFrom').value, '2026-11-01');
  assert.equal(app.elements.get('usageRangeTo').value, '2026-11-03');
  assert.equal(app.elements.get('dailyUsageTableBody').innerHTML, '');
  app.evaluate('applyPresetRange("all")');
  assert.equal(app.elements.get('usageRangeFrom').value, '2026-10-31');
  assert.equal(app.elements.get('usageRangeTo').value, '2026-10-31');
});

test('the explorer selects the highest-cost day and keeps unsupported usage visible', () => {
  const high = row('2026-11-02', 220, '<img src=x onerror=alert(1)>'); high.total_cost_usd = 0.003;
  high.breakdown_rows[0].total_cost_usd = 0.003;
  const app = dashboard({dataset: {providers: {codex: {rows: [high, row('2026-11-03')]}}}});
  assert.equal(app.elements.get('explorerDay').value, '2026-11-02');
  assert.match(app.elements.get('explorerSummary').textContent, /220/);
  assert.match(app.elements.get('explorerProjects').innerHTML, /Unassigned \/ legacy/);
  assert.ok(!app.elements.get('explorerModels').innerHTML.includes('<img'));
  assert.match(app.elements.get('explorerModels').innerHTML, /&lt;img/);
});

test('supported PI sessions and cost-source rows actually render inside their disclosures', () => {
  const day = row('2026-11-03');
  day.investigation_rows = [{...day.breakdown_rows[0], project_id: 'a'.repeat(24), project_label: 'Project aaaaaaaa',
    session_id: 'b'.repeat(24), session_label: 'Session bbbbbbbb', cost_source: 'native'}];
  const app = dashboard({dataset: {providers: {pi: {rows: [day]}}}});
  assert.match(app.elements.get('explorerProjects').innerHTML, /Session bbbbbbbb/);
  assert.match(app.elements.get('explorerProjects').innerHTML, /<td class="num">110<\/td>/);
  assert.match(app.elements.get('valuationDetails').innerHTML, /<td>Native-reported<\/td><td class="num">110<\/td>/);
});

test('matched periods use local calendar boundaries and label new versus unavailable usage', () => {
  const app = dashboard({dataset: {providers: {codex: {rows: [row('2026-10-01'), row('2026-11-03')]}}}});
  const previous = JSON.parse(app.evaluate('JSON.stringify(comparisonRange(parseIsoDate("2026-11-01"), parseIsoDate("2026-11-03"), "mtd").map(toIsoDate))'));
  assert.deepEqual(previous, ['2026-10-01', '2026-10-03']);
  assert.match(app.evaluate('formatDelta(110, 0, false)'), /New usage/);
  app.evaluate('applyPresetRange("mtd")');
  assert.match(app.elements.get('periodComparison').innerHTML, /2026-10-01/);
  assert.match(app.elements.get('periodComparison').innerHTML, /<td>Tokens<\/td>/);
  assert.match(app.elements.get('periodComparison').innerHTML, /codex \/ gpt-6-astra/);
  app.evaluate('applyPresetRange("all")');
  assert.match(app.elements.get('periodComparison').innerHTML, /unavailable|unknown/i);
});

test('bounded retries recover and a successful retry reloads exactly once', async () => {
  let available = false;
  const app = dashboard({fetch: async () => {
    if (!available) throw new Error('offline');
    return {ok: true, json: async () => ({ok: true, updated_at: '2026-11-03T12:00:00Z'})};
  }});
  const runTimer = async () => {const [id, timer] = [...app.timers.entries()][0]; app.timers.delete(id); timer.fn(); await app.tick();};
  await runTimer();
  for (const delay of [15000, 30000, 60000, 120000, 240000, 300000]) {
    assert.equal([...app.timers.values()][0].delay, delay);
    if (delay < 300000) await runTimer();
  }
  available = true;
  await runTimer();
  assert.equal(app.reloads, 1);
  assert.equal(app.elements.get('retryRecalc').hidden, true);
  assert.equal(app.timers.size, 0);
});

test('a hung fetch is aborted after the request deadline and still retries', async () => {
  const app = dashboard({fetch: (url, {signal}) => new Promise((resolve, reject) => signal.addEventListener('abort', () => reject(new Error('timeout'))))});
  const [id, boundary] = [...app.timers.entries()][0]; app.timers.delete(id); boundary.fn();
  const [timeoutId, timeout] = [...app.timers.entries()].find(([, timer]) => timer.delay === 20000);
  app.timers.delete(timeoutId); timeout.fn(); await app.tick();
  assert.equal(app.reloads, 0);
  assert.match(app.elements.get('autoRecalcStatus').textContent, /stale/);
  assert.equal([...app.timers.values()][0].delay, 15000);
});

test('calendar and rolling presets cross years while month comparisons cap short months', () => {
  const january = dashboard({now: '2027-01-03T12:00:00', dataset: {providers: {codex: {rows: [row('2026-12-31')]}}}});
  january.evaluate('applyPresetRange("ytd")');
  assert.equal(january.elements.get('usageRangeFrom').value, '2027-01-01');
  assert.equal(january.elements.get('usageRangeTo').value, '2027-01-03');
  january.evaluate('applyPresetRange("last7")');
  assert.equal(january.elements.get('usageRangeFrom').value, '2026-12-28');
  const march = dashboard({now: '2028-03-31T12:00:00'});
  assert.deepEqual(JSON.parse(march.evaluate('JSON.stringify(comparisonRange(parseIsoDate("2028-03-01"), parseIsoDate("2028-03-31"), "mtd").map(toIsoDate))')), ['2028-02-01', '2028-02-29']);
});
