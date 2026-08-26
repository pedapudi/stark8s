# Workload graph editor

`editor.html` is a single HTML file, with its style sheet and script
inline, that builds, edits, and views a stark8s Workload as a graph. It
converts between the drawing and the Workload YAML in both directions. It
has no build step, loads no library, and fetches no font.

The coordinator serves the same file, and a page served that way reads the
running workload from the coordinator it came from. Those are the only
requests the page makes, they are same-origin GETs, and a page opened any
other way makes none at all.

## Opening it

The file works from any of four places.

- Open `web/editor.html` directly in a browser from the file system.
- Serve the `web` directory with any static file server and open
  `editor.html` from it.
- Publish the file as a hosted page. Downloads may be blocked on some
  hosts; the "copy yaml" control copies the document to the clipboard for
  that case.
- Ask a running workload's coordinator for it, which also shows that
  workload live. See Watching a running workload below.

Appending `?selftest` to the address runs the round-trip check inside the
page (see Testing below).

## Watching a running workload

The coordinator embeds this file and serves it at `/editor`, so viewing a
workload that is running needs no script and no file on disk:

```sh
kubectl port-forward svc/<workload>-coordinator 8080:8080
```

then open `http://127.0.0.1:8080/editor`.

The page reads `GET /topology` once to draw the graph and polls
`GET /metrics` every two seconds to refresh the overlay. It only ever
reads. Nothing is written back, and no request the page makes can change
the workload.

Whether a coordinator is there is detected rather than configured: the
page tries the two addresses relative to where it was served from, and a
failure of either leaves it exactly as it would be opened from a file. So
the same file is both the offline editor and the live viewer.

What the coordinator can and cannot tell you is worth stating, because the
overlay shows less this way than a pasted `kubectl` document does. The
coordinator holds the channel graph and the counters, so every channel
mark is exact. It does not hold the pod templates, the scaling bounds, or
the workload's name, so an operation drawn from the coordinator carries
its name and nothing else. It has no notion of a phase either, so no phase
glyph is drawn. What it does know about an operation is the number of pods
registered with it, which is neither the replica count the controller
asked for nor Kubernetes readiness, and is labelled `live` for that
reason.

Editing still works on a page served this way. The graph is loaded once,
at connect, and only when the page has nothing else open; the poll after
that touches the overlay alone and never rewrites the document. Pressing
"load graph" replaces the document with the coordinator's view on
purpose. Pasting into the status pane takes precedence over the live feed
until the pane is cleared.

## What the page shows

The canvas draws each operation as a node and each channel as an edge. A
layered layout ranks operations by their longest path over the channels
that are not feedback, left to right, and orders the operations within a
rank to reduce crossings. A channel that closes a cycle is drawn as an
arc above the nodes, and a channel from an operation to itself as a loop
on top of the node. A channel with no producer enters from a small
external-producer glyph on its left; a channel with no consumer leaves to
an external-consumer glyph on its right.

The YAML pane on the left holds the Workload document and follows every
edit to the drawing. The properties pane on the right edits the selected
operation or channel. The status strip under the canvas lists validation
failures. The status pane under the YAML pane accepts the output of
`kubectl get workload <name> -o yaml` and overlays the observed state on
the drawing.

## Mark vocabulary

Colour carries direction only: an invalid element and a failed phase take
the theme's bad colour, and the one accent marks the selection. Every
other attribute is a drawn mark. Pointing at a mark opens a card that
names it; the table below is the same vocabulary.

| mark | where | meaning |
|---|---|---|
| short bar across the edge near the consumer | edge | Materialized delivery: a stage barrier; nothing is delivered until the producer has completed |
| plain edge with no bar | edge | Pipelined delivery |
| fan of three slanted strokes at the midpoint | edge | Hash partitioning |
| small ring at the midpoint | edge | RoundRobin partitioning |
| three strokes radiating forward at the midpoint | edge | Broadcast partitioning |
| small square at the producer end | edge | Retained durability |
| arc above the nodes, or a loop on one node, with a faint label giving the epoch bound and mode | edge | feedback channel |
| dashed stroke | edge | the channel is the overflow target of a feedback loop |
| thicker stroke | edge | observed status reports pending records on the channel |
| dashed stroke in the bad colour | edge or node | validation fails for this element |
| tick at the top right of a node | node | Drain completion: the operation finishes once its inbound channels are sealed and consumed |
| ring at the top right of a node | node | Never completion: the operation runs until the workload is deleted |
| circle with a centre dot | edge end | external producer: records arrive through the exchange API |
| circle with a centre bar | edge end | external consumer: records are read through the exchange API |
| empty ring, dotted ring, tick, cross | node, from observed status | phase Waiting, Running, Succeeded, Failed |
| `ready/replicas` text in the node's second line | node, from observed status | ready and total replicas |
| `N live` text in the node's second line | node, from a coordinator | pods registered with the coordinator and not expired; not a replica count and not readiness |
| `N runnable`, `complete` text in the node's second line | node, from a coordinator | partitions with pending input, and whether every inbound channel is drained and every pod has reported done |
| `pending · inflight · produced · epoch` text under an edge | edge, from observed status | the channel's counters |
| chevron at the consumer end | edge | direction of flow |
| accent line down a node's left edge, name in full ink | node | the selected operation |
| accent stroke | edge | the selected channel |

## Editing

- Add an operation with the "add operation" control or by double-clicking
  empty canvas.
- Connect operations by dragging from the port on a node's right edge to
  another node. Dragging to the same node creates a feedback loop.
  Dragging to empty space right of the node creates a channel with no
  consumer. Dragging from the ingress origin at the left creates a channel
  with no producer. A new channel that would close a cycle is created as a
  feedback channel.
- Rename an operation by double-clicking its name.
- Delete the selection with Delete or Backspace.
- Drag a node to place it. A dragged node keeps its place until "auto
  layout" is pressed.
- Zoom with the wheel and pan by dragging empty canvas; "fit" returns to
  the whole drawing.

The properties pane edits an operation's name, completion, slots,
container image, command, resource requests and limits, horizontal and
vertical scaling, and the full pod template as JSON. The image, command,
and resources fields rewrite only those keys of the first container, so a
template pasted from elsewhere keeps every other field. The command field
splits on whitespace; an argument containing a space must be edited in the
pod template JSON. For a channel the pane edits the name, producer,
consumer, partitioning, delivery, durability, and feedback settings.

Keys: Delete removes the selection, Escape clears it, `l` runs the
layout, `f` fits the drawing, `/` focuses the YAML pane.

Theme, page scale, pane sizes, and the current document persist in the
browser's local storage. The page renders correctly with nothing stored.

## Validation

The page applies the checks the controller applies, as one list rather
than at the first failure: duplicate operation and channel names, a
producer or consumer that names no declared operation, a feedback channel
with no consumer, and a cycle that no feedback channel closes. Two further
checks state constraints the runtime documents: a Hash channel whose
consumer may reach more replicas than the channel has partitions, and a
feedback overflow that names an undeclared channel.

## YAML subset

The page contains its own reader and writer for the part of YAML the
Workload schema needs.

Supported: block mappings, block sequences, flow mappings such as
`{mode: Hash, partitions: 4}`, flow sequences such as `[a, b]`, plain
scalars, double-quoted scalars with escapes, single-quoted scalars,
literal (`|`) and folded (`>`) block scalars, comments, and the `---`
and `...` document markers. Plain scalars resolve to null, booleans,
integers, floats, or strings by the YAML 1.2 core schema; `yes`, `no`,
`on`, and `off` stay strings. A parse failure reports a line number under
the pane and the drawing keeps the last document that parsed.

Unsupported: anchors, aliases, tags, multiple documents in one file, and
complex keys.

The writer emits block style, with short collections of scalars three or
more levels deep in flow style, and quotes any string that would otherwise
read as another type. Comments in pasted YAML are kept until the first
edit made through the drawing or the properties pane, after which the
document is regenerated from the model.

## Testing

`roundtrip_test.mjs` runs under Node with no dependencies:

```sh
node web/roundtrip_test.mjs
```

It loads the model script out of `editor.html`, parses
`examples/wordcount/workload.yaml` and `examples/pagerank/workload.yaml`,
compares each with an expected document written by hand, writes each back
out, parses the result, and checks that the two documents are equal. It
also exercises the YAML features listed above, the validation checks, and
the layout's ranking. The same round-trip runs inside the page when it is
opened with `?selftest`; the result appears in the status strip and in the
browser console.
