# Prior work and what this design takes from it

This document surveys the research and production systems that precede a Kubernetes extension in which a workload is a directed graph, cycles permitted, of operations. Each operation owns a pool of pods that scales horizontally and vertically on its own. Each edge is a typed information-flow channel declaring partitioning (hash, range, one-to-one, broadcast), delivery (materialized barrier or pipelined streaming), and durability. Cycles run under epoch semantics. The explicit edge set drives autoscaling from per-edge backlog and generates network policy. The first target workload is the Apache Spark physical plan, whose stages are separated by shuffles.

For every work below the entry gives the citation with venue, year, and URL, states the contribution, and states what the design borrows or where it differs.

## 1. Dataflow foundations

### Kahn process networks

Gilles Kahn, "The Semantics of a Simple Language for Parallel Programming," IFIP Congress 1974, North-Holland, pages 471 to 475. https://dblp.org/rec/conf/ifip/Kahn74.html (PDF: https://perso.ensta-paris.fr/~chapoutot/various/kahn_networks.pdf)

A network of deterministic sequential processes connected by unbounded FIFO channels with blocking reads computes a unique result independent of scheduling. The design borrows the stance that the channel is a first-class object, and it relies on the determinism result: an operation's pool can be resized without changing the computed result as long as each edge preserves order within a partition. Kahn channels are one-to-one and unbounded, whereas the design's edges are many-to-many with explicit partitioning and bounded, backpressured buffers. The design therefore guarantees order per key on a hash edge and leaves global order unspecified.

### Synchronous dataflow

Edward A. Lee and David G. Messerschmitt, "Synchronous Data Flow," Proceedings of the IEEE 75(9):1235 to 1245, September 1987. https://ptolemy.berkeley.edu/publications/papers/87/

Synchronous dataflow restricts each actor to fixed production and consumption rates per firing. Under that restriction static scheduling, buffer bounds, and deadlock freedom are decidable, including for graphs with cycles when a delay token sits on each back-edge. The design borrows the rule that every cycle must carry an initial delay; the epoch boundary on a back-edge plays that role. Rates in analytics workloads depend on data, so the static schedulability theory does not transfer.

### The Volcano exchange operator

Goetz Graefe, "Volcano: An Extensible and Parallel Query Evaluation System," IEEE Transactions on Knowledge and Data Engineering 6(1):120 to 135, 1994. https://dl.acm.org/doi/10.1109/69.273032

Volcano encapsulates parallelism (partitioning, inter-process data movement, flow control) inside one exchange operator so that all other operators remain single-threaded and unaware of parallelism. The typed edge descends directly from the exchange operator, and the partitioning attribute (hash, range, broadcast) is the exchange's partitioning function. Volcano's exchange is pull-based and pipelined within a single address space and leaves durability implicit. The design adds durability and barrier attributes, and it decouples the exchange's degree of parallelism from the operator so that producer and consumer sides scale separately.

## 2. Cluster dataflow systems

### Dryad

Michael Isard, Mihai Budiu, Yuan Yu, Andrew Birrell, and Dennis Fetterly, "Dryad: Distributed Data-Parallel Programs from Sequential Building Blocks," EuroSys 2007, pages 59 to 72. https://dl.acm.org/doi/10.1145/1272996.1273005

Dryad is a general DAG execution engine in which vertices are sequential programs and edges are typed channels (files, TCP pipes, shared-memory FIFOs), with a graph-construction language and runtime graph refinement. Dryad is the closest single precedent for choosing a transport and durability type per edge and for the observation that a channel's durability type determines the cost of fault tolerance. Dryad vertices are single processes with a statically fixed count and the graph is acyclic. The design makes each vertex a resizable pool and permits cycles.

### CIEL

Derek G. Murray, Malte Schwarzkopf, Christopher Smowton, Steven Smith, Anil Madhavapeddy, and Steven Hand, "CIEL: A Universal Execution Engine for Distributed Data-Flow Computing," NSDI 2011. https://www.usenix.org/conference/nsdi11/ciel-universal-execution-engine-distributed-data-flow-computing

CIEL supports dynamic task graphs: a task may spawn tasks and delegate its outputs, which yields iteration and recursion with data-dependent termination while keeping lineage-based fault tolerance. The design borrows the argument that iteration requires control decisions made inside the graph, and the future-reference mechanism for outputs that do not yet exist. CIEL unrolls iteration into fresh tasks, whereas the design keeps a static cyclic graph with epochs; the static form is cheaper per iteration and requires an explicit termination predicate on every back-edge.

### Spark resilient distributed datasets

Matei Zaharia, Mosharaf Chowdhury, Tathagata Das, Ankur Dave, Justin Ma, Murphy McCauley, Michael J. Franklin, Scott Shenker, and Ion Stoica, "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing," NSDI 2012, pages 15 to 28. https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia

RDDs are coarse-grained transformations with lineage. The DAG scheduler cuts a job into stages at wide (shuffle) dependencies and runs each stage as a barrier-separated set of tasks. The narrow versus wide dependency distinction maps onto one-to-one edges versus hash and range edges, and the stage boundary is the materialized-barrier delivery type. A Spark stage is a transient task set on a shared executor pool. The design turns each stage into an operation with its own pool, which enables per-stage resource shaping and also requires that materialized shuffle output live somewhere other than the producing executor (Section 3).

### Naiad and timely dataflow

Derek G. Murray, Frank McSherry, Rebecca Isaacs, Michael Isard, Paul Barham, and Martín Abadi, "Naiad: A Timely Dataflow System," SOSP 2013. https://dl.acm.org/doi/10.1145/2517349.2522738 (PDF: https://sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf)

Timely dataflow is cyclic dataflow in which every message carries a logical timestamp extended with one loop counter per nested cycle, and a distributed progress-tracking protocol tells each operator when no further messages for a given timestamp can arrive. This is the strongest precedent for cycles with epoch semantics: the design's epoch is a timely timestamp with a single loop coordinate and frontier notification at epoch boundaries. Naiad operators are statically partitioned worker threads and the graph cannot be resized at runtime (see Megaphone in Section 5). Timely's per-timestamp frontiers allow pipelined cycles; a global barrier per epoch reduces to the bulk-synchronous model and forfeits that latency advantage, so the design states explicitly which edges inside a cycle are barrier-typed.

### Pregel and the bulk-synchronous parallel model

Grzegorz Malewicz, Matthew H. Austern, Aart J. C. Bik, James C. Dehnert, Ilan Horn, Naty Leiser, and Grzegorz Czajkowski, "Pregel: A System for Large-Scale Graph Processing," SIGMOD 2010, pages 135 to 146. https://dl.acm.org/doi/10.1145/1807167.1807184

Leslie G. Valiant, "A Bridging Model for Parallel Computation," Communications of the ACM 33(8):103 to 111, 1990. https://dl.acm.org/doi/10.1145/79173.79181

Pregel is bulk-synchronous vertex-centric iteration: supersteps separated by global barriers, messages delivered in the following superstep, and vote-to-halt termination, built on Valiant's BSP model. A superstep is an epoch whose back-edge carries a materialized barrier, and vote-to-halt is a clean termination rule the design adopts. Pregel's barrier is global; the design permits pipelined edges inside a cycle and so must define the epoch in terms of the barrier-typed edges.

### Stratosphere iterations and Nephele/PACTs

Stephan Ewen, Kostas Tzoumas, Moritz Kaufmann, and Volker Markl, "Spinning Fast Iterative Data Flows," PVLDB 5(11):1268 to 1279, 2012. https://dl.acm.org/doi/10.14778/2350229.2350245 (PDF: http://vldb.org/pvldb/vol5/p1268_stephanewen_vldb2012.pdf)

Dominic Battré, Stephan Ewen, Fabian Hueske, Odej Kao, Volker Markl, and Daniel Warneke, "Nephele/PACTs: A Programming Model and Execution Framework for Web-Scale Analytical Processing," SoCC 2010, pages 119 to 130. https://dl.acm.org/doi/10.1145/1807128.1807148

Stratosphere adds bulk and incremental (delta) iterations to a DAG engine as first-class operators with a feedback channel; the optimizer treats loop-invariant inputs specially and delta iterations recompute only changed elements. Nephele/PACTs' output contracts are an early form of edge annotation that an optimizer exploits. Flink deprecated the DataSet iteration operators beginning with Flink 1.12 and offers the Flink ML iteration API as the replacement (https://nightlies.apache.org/flink/flink-ml-docs-master/docs/development/iteration/). The design exposes bulk and delta as two back-edge kinds and lets the receiving operation cache loop-invariant inputs. Stratosphere co-locates iteration head and tail on the same parallel instances; independently scaled pools remove that locality, so a back-edge in the design is a full repartitioning exchange.

### MillWheel

Tyler Akidau, Alex Balikov, Kaya Bekiroglu, Slava Chernyak, Josh Haberman, Reuven Lax, Sam McVeety, Daniel Mills, Paul Nordstrom, and Sam Whittle, "MillWheel: Fault-Tolerant Stream Processing at Internet Scale," PVLDB 6(11):1033 to 1044, 2013. https://dl.acm.org/doi/10.14778/2536222.2536229

MillWheel provides per-key computations, low watermarks for progress, and exactly-once delivery through per-record checkpointing and deduplication. Per-edge watermarks are the streaming analogue of epoch completion, and the design's durability attribute distinguishes at-least-once with sender retry from checkpointed exactly-once in the way MillWheel does. MillWheel graphs are acyclic.

### The Dataflow Model

Tyler Akidau, Robert Bradshaw, Craig Chambers, Slava Chernyak, Rafael J. Fernández-Moctezuma, Reuven Lax, Sam McVeety, Daniel Mills, Frances Perry, Eric Schmidt, and Sam Whittle, "The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing," PVLDB 8(12):1792 to 1803, 2015. https://dl.acm.org/doi/10.14778/2824032.2824076

The Dataflow Model unifies batch and streaming: windowing, triggering, and accumulation modes decouple what is computed from when results are emitted. The design's separation of delivery (pipelined or barrier) from computation is the same decomposition; a materialized-barrier edge is a trigger at the watermark of a global window. The Dataflow Model is a programming model and says nothing about per-stage resources.

### Ray and Exoshuffle

Philipp Moritz, Robert Nishihara, Stephanie Wang, Alexey Tumanov, Richard Liaw, Eric Liang, Melih Elibol, Zongheng Yang, William Paul, Michael I. Jordan, and Ion Stoica, "Ray: A Distributed Framework for Emerging AI Applications," OSDI 2018, pages 561 to 577. https://www.usenix.org/conference/osdi18/presentation/moritz

Frank Sifei Luan et al., "Exoshuffle: An Extensible Shuffle Architecture," SIGCOMM 2023. https://dl.acm.org/doi/10.1145/3603269.3604848

Ray combines a dynamic task graph with actors, a distributed object store, and a bottom-up scheduler. Ray's placement groups and per-actor resource requests demonstrate heterogeneous per-vertex resource shapes on one cluster. Ray's graph is implicit and untyped, so its autoscaler reacts to pending resource demands. Exoshuffle shows that a shuffle can be an application-level library over a general object store; the design treats shuffle backends as pluggable implementations of the durable edge type on the same reasoning.

### TensorFlow dynamic control flow

Yuan Yu, Martín Abadi, Paul Barham, Eugene Brevdo, Mike Burrows, Andy Davis, Jeff Dean, Sanjay Ghemawat, Tim Harley, Peter Hawkins, Michael Isard, Manjunath Kudlur, Rajat Monga, Derek G. Murray, and Xiaoqiang Zheng, "Dynamic Control Flow in Large-Scale Machine Learning," EuroSys 2018. https://dl.acm.org/doi/10.1145/3190508.3190551

Martín Abadi et al., "TensorFlow: A System for Large-Scale Machine Learning," OSDI 2016, pages 265 to 283. https://www.usenix.org/conference/osdi16/technical-sessions/presentation/abadi

TensorFlow expresses cyclic graphs with Switch, Merge, Enter, Exit, and NextIteration primitives; frames tagged with iteration counters let several iterations be in flight across devices, and loops are partitioned across machines automatically with inserted control edges. The frame and iteration tag is an independent derivation of timely dataflow's loop counters, and the rule for partitioning a loop body across devices is what the design applies when a cycle spans pools. TensorFlow placement is static.

### Apache Tez

Bikas Saha, Hitesh Shah, Siddharth Seth, Gopal Vijayaraghavan, Arun Murthy, and Carlo Curino, "Apache Tez: A Unifying Framework for Modeling and Building Data Processing Applications," SIGMOD 2015, pages 1357 to 1369. https://dl.acm.org/doi/10.1145/2723372.2742790

Tez types every DAG edge along three axes: data movement (one-to-one, broadcast, scatter-gather), scheduling (sequential or concurrent), and data source (persisted, persisted-reliable, ephemeral). VertexManager plugins reconfigure a vertex's parallelism at runtime from upstream statistics. Tez's three axes are the closest published match to the design's partitioning, delivery, and durability triple, and its runtime reduction of parallelism is the ancestor of Spark's adaptive partition coalescing. Tez runs on YARN containers pooled per application and its graphs are acyclic.

### Hyracks

Vinayak R. Borkar, Michael J. Carey, Raman Grover, Nicola Onose, and Rares Vernica, "Hyracks: A Flexible and Extensible Foundation for Data-Intensive Computing," ICDE 2011, pages 1151 to 1162. https://ieeexplore.ieee.org/document/5767921

Hyracks jobs are DAGs of operators and connectors, where connectors are the repartitioning objects (hash, range, one-to-one, broadcast) and are pluggable. The operator versus connector vocabulary is the cleanest published statement that edges are objects with their own implementations, and Hyracks splits a job into stages by connector type, which is the materialized versus pipelined decision.

## 3. Spark-specific work

### Adaptive Query Execution

Databricks, "Adaptive Query Execution: Speeding Up Spark SQL at Runtime," May 2020. https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html

Databricks, "Introducing Apache Spark 3.0," June 2020. https://www.databricks.com/blog/2020/06/18/introducing-apache-spark-3-0-now-available-in-databricks-runtime-7-0.html

Adaptive Query Execution re-plans at each shuffle boundary from materialized shuffle statistics: it coalesces small partitions, switches sort-merge joins to broadcast joins, and splits skewed partitions. A materialized-barrier edge is therefore also an observation point. The design exposes per-edge partition-size statistics to its controller so the downstream pool size and partition count are chosen after the upstream barrier completes. Adaptive Query Execution changes only the plan; executor shapes per stage require stage-level scheduling.

### Stage-level scheduling

SPARK-27495, "SPIP: Support Stage level resource configuration and scheduling," released in Spark 3.1.1. https://issues.apache.org/jira/browse/SPARK-27495

An RDD may carry a ResourceProfile, and with dynamic allocation Spark acquires distinct executors per profile. This is Spark's own mechanism for per-stage resource shape and the hook through which an operation's pool maps to Spark executors. Profiles must match exactly, executors are never shared across profiles, and shuffle data written by one profile's executors must remain readable after those executors are released; the design faces the same constraint.

### Magnet

Min Shen, Ye Zhou, and Chandni Singh, "Magnet: Push-based Shuffle Service for Large-scale Data Processing," PVLDB 13(12):3382 to 3395, 2020. https://dl.acm.org/doi/10.14778/3415478.3415558 (PDF: https://www.vldb.org/pvldb/vol13/p3382-shen.pdf)

Mappers push shuffle blocks to remote shuffle services that merge them per reduce partition; reducers read large sequential merged files with locality and fall back to pull for unmerged blocks. Magnet is merged into Apache Spark 3.2 as push-based shuffle. An edge with external durability and hash partitioning is a push-merge shuffle, and merging per partition is what makes the consumer pool's size independent of the producer pool's size. Magnet couples to YARN node managers; on Kubernetes the shuffle service is its own pool, which the design represents as a system-owned operation.

### Riffle

Haoyu Zhang, Brian Cho, Ergin Seyfe, Avery Ching, and Michael J. Freedman, "Riffle: Optimized Shuffle Service for Large-Scale Data Analytics," EuroSys 2018. https://dl.acm.org/doi/10.1145/3190508.3190534

Riffle merges small map outputs into large block files, converting random disk I/O into sequential I/O. It gives the quantitative case that shuffle I/O breaks first when map task count grows, which is what happens if a producer pool is enlarged naively.

### Apache Celeborn, Apache Uniffle, and Uber's remote shuffle service

Apache Uniffle incubator proposal: https://cwiki.apache.org/confluence/display/INCUBATOR/UniffleProposal. Tencent Firestorm, the origin of Uniffle: https://github.com/Tencent/Firestorm. Uber RemoteShuffleService: https://github.com/uber/RemoteShuffleService. Celeborn deployment on Amazon EMR on EKS: https://aws.amazon.com/blogs/big-data/high-performance-remote-shuffle-service-on-amazon-emr-with-apache-celeborn/

Celeborn (Alibaba origin) and Uniffle (Tencent origin, an ASF top-level project since March 2025) are production remote shuffle services with master and worker roles, push-based writes, replication, and Kubernetes deployments. These are the concrete implementations of the durable externally materialized edge type, and the design treats them as pluggable edge backends. None of them exposes per-partition backlog as an autoscaling signal to Kubernetes.

### Dynamic allocation on Kubernetes

SPARK-24432, umbrella issue for dynamic resource allocation in Kubernetes mode: https://issues.apache.org/jira/browse/SPARK-24432. Spark on Kubernetes documentation: https://spark.apache.org/docs/latest/running-on-kubernetes.html

Kubernetes has no external shuffle service for Spark, so `spark.dynamicAllocation.shuffleTracking.enabled` keeps executors alive while they hold shuffle files for active jobs. A producer pool can shrink to zero after its barrier completes only when the edge's durability is external; with local shuffle files the producer's lifetime is bound to consumer completion, and the edge type in the design records which case applies.

## 4. Kubernetes-side systems

### Argo Workflows

https://argo-workflows.readthedocs.io/en/latest/ and https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/

Argo is a container-native DAG and steps engine in which edges are control dependencies plus artifacts passed through a repository. Nodes are single pods or fixed fan-outs, edges carry no partitioning or streaming semantics, cycles appear only through recursive templates, and nothing resizes a node's pod count from backlog.

### Volcano

https://volcano.sh/docs/concepts/podgroup/ and https://github.com/volcano-sh/volcano/blob/master/docs/design/task-minavailable.md

Volcano provides the PodGroup resource with `minAvailable` and `minTaskMember` gang scheduling, queues, and dominant-resource fairness. A pipelined-streaming edge requires both endpoints running at once, so the pair, or the whole strongly connected component of a cycle, is a gang unit, and PodGroup is the mechanism.

### Kueue

https://kueue.sigs.k8s.io and https://kubernetes.io/blog/2022/10/04/introducing-kueue/

Kueue performs quota-based admission and queueing of Workloads (Job, JobSet, RayJob, and others) before pods are created. The design's controller emits a Kueue Workload per operation or per barrier-separated phase so that admission is quota-aware. Kueue has no model of inter-operation edges.

### JobSet and LeaderWorkerSet

https://github.com/kubernetes-sigs/jobset and https://github.com/kubernetes-sigs/lws

JobSet groups ReplicatedJobs with startup ordering and headless-service DNS; LeaderWorkerSet deploys a leader with workers as one replication unit. A ReplicatedJob is the nearest stock primitive for one operation's pool, and JobSet's startup order is a control-only edge. The design contributes the data-carrying edge these lack.

### KEDA

https://www.k8s.guide/ecosystem/keda/

KEDA scales Deployments and Jobs on external metrics such as Kafka consumer-group lag, including scale to zero, through a horizontal pod autoscaler it manages. KEDA is the established Kubernetes idiom for backlog-driven scaling, and each typed edge publishes a lag metric that a per-operation ScaledObject can consume. KEDA scales one workload against one queue and has no model of how scaling a consumer shifts backlog downstream (see DS2 in Section 5).

### Flink Kubernetes Operator autoscaler

FLIP-271: https://cwiki.apache.org/confluence/display/FLINK/FLIP-271:+Autoscaling. Documentation: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/

The operator scales each job vertex independently from observed true processing rate, busy time, and source backlog, with a catch-up-time budget, and back-propagates rate limits from non-scalable bottlenecks upstream (FLINK-31215). This is the production implementation of per-edge backlog driving per-operation parallelism, using the DS2 model. Flink vertices share TaskManager pods, a parallelism change restarts the job from a savepoint, and DataStream iteration support is limited.

### KubeRay

https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html

A RayCluster resource declares several worker groups, each with its own pod template and minimum and maximum replicas; the Ray autoscaler raises `replicas` on demand. Worker groups are heterogeneous per-role pools and an existing pattern for the design's per-operation pool. Scaling reacts to pending task and actor resource requests.

### Apache YuniKorn

https://yunikorn.apache.org/docs/user_guide/gang_scheduling/

YuniKorn provides gang scheduling with task groups (for Spark, a driver group and an executor group), hierarchical queues, and placeholder pods that reserve capacity. Task groups map onto operations, and placeholder reservation guarantees that a consumer pool exists before a pipelined edge starts flowing.

### Scheduling research on DAG workloads

Hongzi Mao, Malte Schwarzkopf, Shaileshh Bojja Venkatakrishnan, Zili Meng, and Mohammad Alizadeh, "Learning Scheduling Algorithms for Data Processing Clusters" (Decima), SIGCOMM 2019. https://arxiv.org/abs/1810.01963

"Carbon- and Precedence-Aware Scheduling for Data Processing Clusters," 2025. https://arxiv.org/abs/2502.09717

"Learning to Schedule: A Supervised Learning Framework for Network-Aware Scheduling of Data-Intensive Workloads," 2025. https://arxiv.org/abs/2510.21419

Decima learns per-stage parallelism and ordering for Spark DAGs and shows large gains from choosing each stage's degree of parallelism separately from a job-wide executor count, which supports per-operation scaling. The two 2025 papers perform stage-level scaling and network-aware placement of Spark on Kubernetes as proofs of concept. None of these exposes edges as Kubernetes objects.

## 5. Autoscaling from backlog and backpressure

### DS2

Vasiliki Kalavri, John Liagouris, Moritz Hoffmann, Desislava Dimitrova, Matthew Forshaw, and Timothy Roscoe, "Three Steps Is All You Need: Fast, Accurate, Automatic Scaling Decisions for Distributed Streaming Dataflows," OSDI 2018. https://www.usenix.org/conference/osdi18/presentation/kalavri

DS2 models a dataflow as a graph of operators with measured true processing and output rates (useful work time, excluding waiting), then solves for the parallelism of every operator at once so that no edge has backpressure, propagating rate changes downstream. It converges in one to three steps where Dhalion needs six. DS2 is the algorithm behind the design's edge-backlog autoscaler; the explicit edge set is the input DS2 needs, and all pool sizes are computed jointly. DS2 assumes linear scaling per operator and acyclic graphs; a cyclic graph requires the rate equations to be solved per epoch, and operators with large shared state or skew scale non-linearly.

### Dhalion

Avrilia Floratou, Ashvin Agrawal, Bill Graham, Sriram Rao, and Karthikeyan Ramasamy, "Dhalion: Self-Regulating Stream Processing in Heron," PVLDB 10(12):1825 to 1836, 2017. https://dl.acm.org/doi/10.14778/3137765.3137786

Dhalion is a symptom-detector, diagnoser, and resolver policy framework that detects backpressure and scales one bottleneck at a time. The design borrows its diagnosis vocabulary (backpressure, skew, slow instance) because a per-edge backlog can rise for reasons that scaling cannot fix.

### Google Cloud Dataflow autoscaling

https://docs.cloud.google.com/dataflow/docs/horizontal-autoscaling

Dataflow scales streaming workers on estimated backlog seconds (target below 15 seconds) combined with CPU utilization, and a separate vertical autoscaler adjusts worker memory. Backlog in seconds (bytes divided by measured throughput) is the signal each edge reports in the design, and the coexistence of horizontal and vertical autoscaling is a precedent for pools that scale along both axes. Dataflow scales the whole pipeline's worker pool.

### Chi

Luo Mai, Kai Zeng, Rahul Potharaju, Le Xu, Steve Suh, Shivaram Venkataraman, Paolo Costa, Terry Kim, Saravanan Muthukrishnan, Vamsi Kuppa, Sudheer Dhulipalla, and Sriram Rao, "Chi: A Scalable and Programmable Control Plane for Distributed Stream Processing Systems," PVLDB 11(10):1303 to 1316, 2018. https://dl.acm.org/doi/10.14778/3231751.3231765

Chi embeds control messages in the data-plane channels so reconfiguration, including scaling, is coordinated by the same ordering the data uses. For pipelined edges the design propagates a rescale-at-epoch marker along the edge to change pool size without draining.

### Megaphone

Moritz Hoffmann, Andrea Lattuada, Frank McSherry, Vasiliki Kalavri, John Liagouris, and Timothy Roscoe, "Megaphone: Latency-Conscious State Migration for Distributed Streaming Dataflows," PVLDB 12(9), 2019. https://arxiv.org/abs/1812.01371

Megaphone performs fine-grained, pre-planned state migration on timely dataflow so that rescaling stateful operators avoids latency spikes. Resizing a pool that owns keyed state on a hash edge is a state-migration problem, so the design specifies whether operations may hold keyed state across epochs and how the hash edge's partition map is versioned.

### StreamCloud

Vincenzo Gulisano, Ricardo Jiménez-Peris, Marta Patiño-Martínez, Claudio Soriente, and Patrick Valduriez, "StreamCloud: An Elastic and Scalable Data Streaming System," IEEE Transactions on Parallel and Distributed Systems 23(12), 2012. https://dl.acm.org/doi/10.1109/TPDS.2012.24

StreamCloud splits queries into subqueries at stateful-operator boundaries and scales each subquery independently, the same decomposition the design applies at exchanges.

## 6. Serverless and disaggregated dataflow

### Locus

Qifan Pu, Shivaram Venkataraman, and Ion Stoica, "Shuffling, Fast and Slow: Scalable Analytics on Serverless Infrastructure," NSDI 2019, pages 193 to 206. https://www.usenix.org/conference/nsdi19/presentation/pu

Locus runs shuffle-heavy analytics on stateless functions by mixing slow object storage with a small fast in-memory tier, with a cost and performance model that selects the mix. When producer and consumer pools are disaggregated, the edge's durability backend is a cost and latency knob, and Locus supplies the model for choosing it per edge. Locus also shows that shuffle storage bandwidth becomes the bottleneck once compute is ephemeral, so the shuffle backend in the design scales with the pools it serves.

### Lambada

Ingo Müller, Renato Marroquín, and Gustavo Alonso, "Lambada: Interactive Data Analytics on Cold Data Using Serverless Cloud Infrastructure," SIGMOD 2020, pages 115 to 130. https://dl.acm.org/doi/10.1145/3318464.3389758

Lambada is purely serverless query execution with an exchange operator over object storage and recursive function invocation for fast fan-out. Its two-level exchange bounds the number of objects written per shuffle, which applies to any object-store-backed edge.

### Runtime plan rewriting and cross-engine mapping

Qifa Ke, Michael Isard, and Yuan Yu, "Optimus: A Dynamic Rewriting Framework for Data-Parallel Execution Plans," EuroSys 2013. https://dl.acm.org/doi/10.1145/2465351.2465354

Ionel Gog, Malte Schwarzkopf, Natacha Crooks, Matthew P. Grosvenor, Allen Clement, and Steven Hand, "Musketeer: All for One, One for All in Data Processing Systems," EuroSys 2015. https://dl.acm.org/doi/10.1145/2741948.2741968

Kay Ousterhout, Ryan Rasti, Sylvia Ratnasamy, Scott Shenker, and Byung-Gon Chun, "Making Sense of Performance in Data Analytics Frameworks," NSDI 2015. https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/ousterhout

Optimus rewrites the Dryad execution graph at runtime from observed statistics. Musketeer maps one workflow DAG onto several back-end engines per subgraph, which supports hosting plans from more than one engine in the design's representation. Ousterhout et al. measure that CPU is the dominant bottleneck in Spark SQL workloads and that faster networks yield a median speedup of at most about 2 percent; the design's justification therefore rests on resource shaping per stage and on scale to zero of finished stages.

## Gaps in prior work that this design addresses

- No surveyed system exposes the dataflow edge as a Kubernetes-level object. Tez, Hyracks, and Dryad type their edges inside a single engine; Argo, JobSet, and Kueue see only control dependencies. Making the typed edge a cluster resource lets one declaration drive both autoscaling and NetworkPolicy.
- No surveyed system combines per-vertex pods with independent horizontal and vertical scaling. The Flink operator scales per-vertex parallelism within shared TaskManager pods; KubeRay has heterogeneous worker groups without a dataflow graph; Spark stage-level scheduling has per-stage executor shapes without per-stage lifecycle or scale to zero.
- No surveyed system performs joint, graph-wide scaling over a cyclic graph. DS2 and the Flink autoscaler are acyclic; Naiad and TensorFlow support cycles without rescaling. Scaling an operation inside a cycle from its inbound backlog, with the epoch barrier bounding when replicas may change, has no precedent found; the implementation scales on backlog only and does not solve DS2-style rate equations.
- No surveyed system treats the Spark physical plan as a target of a general edge-typed representation managed outside Spark. Musketeer and Tez map many front ends onto one engine; mapping Spark stages onto externally managed pools and edges has no precedent found.

## Claims in the design that prior work refutes or complicates

- Independent producer and consumer scaling requires an external, durable shuffle. Spark on Kubernetes without a remote shuffle service keeps producers alive through shuffle tracking, and Magnet, Celeborn, Uniffle, Riffle, and Locus all exist because local shuffle output couples pool lifetimes and because shuffle storage becomes the bottleneck once compute is disaggregated. The implementation uses one in-memory exchange per workload; a production deployment needs a remote shuffle service as its own scaled pool.
- Backlog-driven scaling alone is insufficient. Dhalion's taxonomy shows backlog rising from skew and slow instances, which more pods leave unresolved; DS2 assumes linear per-operator scaling and warns that large state and skew break the model; Megaphone shows that rescaling stateful operators is a migration problem. Adaptive Query Execution handles skew at the plan level. The exchange reports per-channel backlog; per-partition statistics, which a planner would need to split skewed partitions, are not yet exposed.
- Pipelined edges and autoscaling conflict with gang scheduling. A pipelined-streaming edge needs both endpoints up, and scaling one endpoint independently changes the gang. The implementation has no gang scheduling: operations start independently and a pipelined consumer simply waits for records. A gang unit equal to the strongly connected component of pipelined edges is the natural extension.
- A global epoch barrier reduces cycles to Pregel and forfeits Naiad's advantage. Timely dataflow's per-timestamp frontiers give low-latency iteration; a global materialized barrier on the back-edge accepts bulk-synchronous latency. The implementation is bulk-synchronous only: a feedback channel advances one epoch at a time for all consumers.
- Arguments from shuffle-network speed are weak. Ousterhout et al. measure at most about 2 percent median speedup from faster networks in Spark SQL workloads; the benefit of per-stage pools is CPU and memory shaping per stage, scale to zero of finished stages, and per-stage parallelism chosen from the size of the sealed input.
- General iteration primitives in batch DAG engines saw limited uptake. Flink deprecated its DataSet bulk and delta iterations in favor of a narrower Flink ML iteration API. Cycle support here is justified by specific workloads (iterative machine learning, graph algorithms, fixed-point queries) rather than by generality.
