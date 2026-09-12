// Round-trip test for the editor's YAML subset and Workload model.
//
// Run with: node web/roundtrip_test.mjs
//
// The test loads the model script block out of web/editor.html (the block
// marked id="core", which touches no DOM), parses the two example
// workloads under examples/, compares the result with an expected object
// written by hand, writes the document back out, parses that, and checks
// that nothing changed. It also exercises the YAML features the CRD schema
// needs and checks that the copies of the examples embedded in the page
// match the files.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');
const html = readFileSync(join(here, 'editor.html'), 'utf8');

const coreSrc = /<script id="core">([\s\S]*?)<\/script>/.exec(html)[1];
const Core = new Function(coreSrc + '\nreturn Core;')();

let failures = 0;
function check(name, ok, detail) {
  console.log(`${ok ? 'pass' : 'FAIL'}  ${name}${!ok && detail ? '\n      ' + detail : ''}`);
  if (!ok) failures++;
}

function container(cmd) {
  return { name: 'main', image: 'stark8s:dev', imagePullPolicy: 'IfNotPresent', command: cmd };
}

const expected = {
  wordcount: {
    apiVersion: 'stark8s.io/v1alpha1', kind: 'Workload', metadata: { name: 'wordcount' },
    spec: {
      operations: [
        { name: 'read', template: { spec: { containers: [container(['/wordcount', 'read'])] } } },
        { name: 'map', slots: 2, scaling: { horizontal: { min: 1, max: 4 } }, template: { spec: { containers: [container(['/wordcount', 'map'])] } } },
        { name: 'reduce', slots: 2, scaling: { horizontal: { min: 1, max: 3 } }, template: { spec: { containers: [container(['/wordcount', 'reduce'])] } } },
      ],
      channels: [
        { name: 'lines', from: 'read', to: 'map', partitioning: { mode: 'RoundRobin', partitions: 8 }, delivery: 'Pipelined' },
        { name: 'shuffle', from: 'map', to: 'reduce', partitioning: { mode: 'Hash', partitions: 6 }, delivery: 'Materialized' },
        { name: 'totals', from: 'reduce' },
      ],
    },
  },
  pagerank: {
    apiVersion: 'stark8s.io/v1alpha1', kind: 'Workload', metadata: { name: 'pagerank' },
    spec: {
      operations: [
        { name: 'seed', template: { spec: { containers: [container(['/pagerank', 'seed'])] } } },
        { name: 'rank', slots: 2, scaling: { horizontal: { min: 2, max: 2 } }, template: { spec: { containers: [container(['/pagerank', 'rank'])] } } },
      ],
      channels: [
        { name: 'graph', from: 'seed', to: 'rank', partitioning: { mode: 'Hash', partitions: 4 }, delivery: 'Materialized' },
        { name: 'contrib', from: 'rank', to: 'rank', partitioning: { mode: 'Hash', partitions: 4 }, feedback: { maxEpochs: 20 } },
        { name: 'ranks', from: 'rank' },
      ],
    },
  },
};

for (const name of ['wordcount', 'pagerank']) {
  const text = readFileSync(join(root, 'examples', name, 'workload.yaml'), 'utf8');
  const parsed = Core.fromYAML(text);
  check(`${name}: parses to the expected document`, Core.deepEqual(parsed, expected[name]), JSON.stringify(parsed));
  const rt = Core.roundTrip(text);
  check(`${name}: emitted yaml parses back to the same document`, rt.ok, rt.yaml);
  check(`${name}: emitted yaml is stable on a second pass`, Core.toYAML(rt.second) === rt.yaml);
  check(`${name}: validates`, Core.validate(parsed).length === 0, JSON.stringify(Core.validate(parsed)));

  const embedded = new RegExp(`<script class="example" type="text/plain" data-name="${name}">\\n([\\s\\S]*?)</script>`).exec(html);
  check(`${name}: embedded copy matches examples/${name}/workload.yaml`, !!embedded && embedded[1] === text);
}

// YAML subset features the CRD schema and pod templates need.
const features = Core.parseYAML(`
---
# leading comment
a: 1            # trailing comment
b: "quoted # not a comment"
c: 'single ''quoted'''
d: [1, two, "three", {x: y}]
e: {p: 1, q: [a, b], r: {s: t}}
f:
  - - nested
    - seq
  - key: value
    other: 2
g: |
  line one
  line two
h: >-
  folded
  text
i: true
j: null
k: ~
l:
- shorthand
- list
m: -1.5
n: 0
o: "a:b"
p: a:b
q: http://example.com/path
`);
check('yaml: scalars and comments', features.a === 1 && features.b === 'quoted # not a comment' && features.c === "single 'quoted'");
check('yaml: flow collections', Core.deepEqual(features.d, [1, 'two', 'three', { x: 'y' }]) && Core.deepEqual(features.e, { p: 1, q: ['a', 'b'], r: { s: 't' } }));
check('yaml: nested block sequences', Core.deepEqual(features.f, [['nested', 'seq'], { key: 'value', other: 2 }]));
check('yaml: block scalars', features.g === 'line one\nline two\n' && features.h === 'folded text');
check('yaml: typed scalars', features.i === true && features.j === null && features.k === null && features.m === -1.5 && features.n === 0);
check('yaml: sequence at the key indent', Core.deepEqual(features.l, ['shorthand', 'list']));
check('yaml: colons inside plain scalars', features.o === 'a:b' && features.p === 'a:b' && features.q === 'http://example.com/path');
check('yaml: features round-trip', Core.deepEqual(Core.parseYAML(Core.emitYAML(features)), features), Core.emitYAML(features));

// Emitter quoting: strings that would read as another type stay strings.
const tricky = { a: '1', b: 'true', c: 'null', d: '', e: ' padded ', f: 'has: colon', g: 'a, b', h: '# hash', i: 'multi\nline', j: 'yes' };
check('yaml: quoted strings survive', Core.deepEqual(Core.parseYAML(Core.emitYAML(tricky)), tricky), Core.emitYAML(tricky));

// Errors carry a line number.
try { Core.parseYAML('a: 1\nb: [1, 2\nc: 3\n'); check('yaml: unbalanced flow is an error', false); }
catch (e) { check('yaml: unbalanced flow is an error with a line', e instanceof Core.YAMLError && e.line === 2, `line ${e.line}: ${e.message}`); }
try { Core.parseYAML('a: 1\n\tb: 2\n'); check('yaml: tab indentation is an error', false); }
catch (e) { check('yaml: tab indentation is an error with a line', e.line === 2, e.message); }

// Validation matches the controller.
const bad = Core.fromYAML(`
spec:
  operations:
    - {name: a, scaling: {horizontal: {max: 4}}, template: {}}
    - {name: b, template: {}}
    - {name: b, template: {}}
  channels:
    - {name: ab, from: a, to: b}
    - {name: ba, from: b, to: a, partitioning: {mode: Hash, partitions: 2}}
    - {name: ab, from: a, to: nowhere}
    - {name: loop, from: a, to: a, feedback: {mode: Asynchronous, maxEpochs: 3, overflow: missing}}
`);
const msgs = Core.validate(bad).map((e) => e.message);
check('validate: duplicate operation', msgs.some((m) => m.includes('duplicate operation "b"')), msgs.join('\n'));
check('validate: duplicate channel', msgs.some((m) => m.includes('duplicate channel "ab"')));
check('validate: unknown consumer', msgs.some((m) => m.includes('unknown consumer "nowhere"')));
check('validate: cycle without feedback', msgs.some((m) => m.includes('has no feedback channel')));
check('validate: hash partitions below replica bound', msgs.some((m) => m.includes('may reach 4 replicas but has 2 partitions')));
check('validate: overflow names an undeclared channel', msgs.some((m) => m.includes('undeclared channel "missing"')));

// Layout ranks by longest path and honours pinned positions.
const wc = Core.fromYAML(readFileSync(join(root, 'examples', 'wordcount', 'workload.yaml'), 'utf8'));
const nodes = Core.layout(wc, { map: { x: 999, y: 7 } });
check('layout: ranks read < map < reduce', nodes.get('read').rank === 0 && nodes.get('map').rank === 1 && nodes.get('reduce').rank === 2);
check('layout: pinned position wins', nodes.get('map').x === 999 && nodes.get('map').y === 7);
const pr = Core.fromYAML(readFileSync(join(root, 'examples', 'pagerank', 'workload.yaml'), 'utf8'));
check('layout: feedback edges do not affect rank', Core.layout(pr, {}).get('rank').rank === 1);

// Capacity planning: replicas = clamp(ceil(runnableTasks / slots), min, max).
{
  const doc = Core.fromYAML(readFileSync(join(root, 'examples', 'wordcount', 'workload.yaml'), 'utf8'));
  const at = (load) => (name) => Core.expectedShape(doc, name, () => load);
  const heavy = at(1000);
  check('capacity: map saturates its bound at high load', heavy('map').replicas === 4 && heavy('map').runnable === 8);
  check('capacity: reduce sizes from ceil(partitions/slots)', heavy('reduce').replicas === 3 && heavy('reduce').raw === 3);
  check('capacity: source runs at its floor', heavy('read').source === true && heavy('read').replicas === 1);
  const light = at(3);
  check('capacity: low load lowers the count', light('map').replicas === 2 && light('reduce').replicas === 2);
  const idle = at(0);
  check('capacity: no load holds map at its min of 1', idle('map').replicas === 1 && idle('map').runnable === 0);
}

// Reading a coordinator's view. apiBase decides whether there is a control
// API to talk to at all, and where it is.
check('apiBase: served at /editor', Core.apiBase('http:', '/editor') === '');
check('apiBase: served under a prefix', Core.apiBase('https:', '/some/prefix/editor') === '/some/prefix');
check('apiBase: a static server serving the file', Core.apiBase('http:', '/web/editor.html') === '/web');
check('apiBase: the file system has nothing to connect to', Core.apiBase('file:', '/home/x/editor.html') === null);

// observedDocument turns a topology into a Workload the editor can draw.
{
  const topology = [
    { name: 'counts', from: 'map', to: 'reduce', partitioning: { mode: 'Hash', partitions: 8 }, delivery: 'Materialized' },
    { name: 'lines', from: 'ingest', to: 'map', partitioning: { mode: 'RoundRobin', partitions: 4 }, delivery: 'Pipelined' },
    { name: 'results', from: 'reduce', partitioning: { mode: 'RoundRobin', partitions: 8 }, durability: 'Retained' },
  ];
  const metrics = { operations: [{ name: 'map' }, { name: 'reduce' }, { name: 'ingest' }, { name: 'lonely' }] };
  const doc = Core.observedDocument(topology, metrics);
  check('observed: every operation the graph mentions appears',
    doc.spec.operations.map((o) => o.name).join(',') === 'map,reduce,ingest,lonely');
  check('observed: operations carry a name and nothing invented',
    doc.spec.operations.every((o) => Object.keys(o).length === 1 && o.name));
  check('observed: the workload is left unnamed', doc.metadata.name === undefined);
  check('observed: channels keep their attributes',
    Core.deepEqual(doc.spec.channels[0], { name: 'counts', from: 'map', to: 'reduce', partitioning: { mode: 'Hash', partitions: 8 }, delivery: 'Materialized' }));
  check('observed: an absent consumer stays absent', doc.spec.channels[2].to === undefined);
  // The document has to survive the editor's own reader and writer, because
  // that is what the viewer does with it.
  const round = Core.fromYAML(Core.toYAML(doc));
  check('observed: the document round-trips', Core.deepEqual(round.spec.channels, doc.spec.channels));
  check('observed: the drawing ranks it', Core.layout(round, {}).get('reduce').rank === 2);
  check('observed: an empty topology is still a document',
    Core.observedDocument([], null).spec.operations.length === 0);
}

// observedStatus reports what a coordinator knows and nothing else. The
// operation figures are the point: phase, ready and replicas are absent
// because a coordinator has none of them.
{
  const st = Core.observedStatus({
    channels: [{ name: 'counts', sealed: true, pending: 42, inFlight: 1, produced: 59, epoch: 2, overflowed: 3, lost: 0 }],
    operations: [{ name: 'map', livePods: 2, runnableTasks: 5, complete: false, holdsUnconsumed: true }],
  });
  check('observed: channel counters map one for one',
    Core.deepEqual(st.channels[0], { name: 'counts', sealed: true, pending: 42, inFlight: 1, produced: 59, epoch: 2, overflowed: 3, lost: 0 }));
  check('observed: operation figures are reported under their own names',
    st.operations[0].livePods === 2 && st.operations[0].runnableTasks === 5 && st.operations[0].holdsUnconsumed === true);
  check('observed: no phase is invented', st.operations[0].phase === undefined);
  check('observed: no replica count is invented',
    st.operations[0].replicas === undefined && st.operations[0].ready === undefined);
  check('observed: empty metrics give empty lists',
    Core.observedStatus(null).channels.length === 0 && Core.observedStatus({}).operations.length === 0);
}


// Whether a coordinator may replace what is on screen. This is the decision
// the viewer gets wrong if it asks "is the document empty": the page loads an
// example during start-up, so by the time an asynchronous connection resolves
// the document is never empty and the example is defended as if a reader had
// written it.
{
  const example = Core.fromYAML(readFileSync(join(root, 'examples', 'wordcount', 'workload.yaml'), 'utf8'));
  const empty = Core.blankWorkload('untitled');
  check('adopt: the untouched example gives way', Core.coordinatorMayAdopt(Core.SEEDS.example, example) === true);
  check('adopt: a document restored from storage is kept', Core.coordinatorMayAdopt(Core.SEEDS.restored, example) === false);
  check('adopt: a document the reader edited is kept', Core.coordinatorMayAdopt(Core.SEEDS.user, example) === false);
  check('adopt: an empty document gives way whatever its seed',
    Core.coordinatorMayAdopt(Core.SEEDS.user, empty) === true && Core.coordinatorMayAdopt(Core.SEEDS.restored, empty) === true);
  check('adopt: a missing document is not an obstacle', Core.coordinatorMayAdopt(Core.SEEDS.user, null) === true);
  // The seed names are part of the contract between the page and this check.
  check('adopt: the seeds are the three the page records',
    Core.SEEDS.example === 'example' && Core.SEEDS.restored === 'restored' && Core.SEEDS.user === 'user');
}

console.log(failures ? `\n${failures} failure(s)` : '\nall checks passed');
process.exit(failures ? 1 : 0);
