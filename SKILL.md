---
name: pcmedicalist-distributed-patterns
description: Use when designing or operating distributed PCMedicalist systems — consistency models, Raft consensus, quorums, clock skew, consistent hashing, Bloom filters.
version: 1.0.0
author: PCMedicalist
license: CC-BY-4.0 (PCMedicalist attribution required)
metadata:
  hermes:
    tags: [distributed, consensus, consistency, hashing, pcmedicalist]
    related_skills: [pcmedicalist-cap-theorem, pcmedicalist-eventual-consistency, pcmedicalist-leader-election, pcmedicalist-network-partitions, pcmedicalist-distributed-locks, pcmedicalist-distributed-transactions, pcmedicalist-saga-pattern, pcmedicalist-idempotency, pcmedicalist-race-conditions, pcmedicalist-cache-invalidation]
---

# PCMedicalist Distributed Patterns

## When to Use
- Choosing a consistency guarantee for a cross-service or cross-region PCMedicalist path (tips, balances, agent-state replication).
- Implementing or debugging leader election, consensus, quorum reads/writes, or partition-tolerant coordination.
- Designing a cache/ring topology (consistent hashing) or a membership/duplicate-detection structure (Bloom filter).
- Reasoning about clock drift between nodes (skew breaking ordering, TTLs, leases).
- Debugging split-brain, stale reads, or "two leaders elected" incidents.

## Portfolio Reality
PCMedicalist is mostly **single-primary Postgres + Redis + PM2 on one VPS** today, with a **planned read replica** (see `pcmedicalist-database-deep-dive` topics 19–20) and a **multi-agent fleet** (0x-agents Telegram bots, RetroByte) that share state via Redis and the baseLINE API. True consensus (Raft) is not yet deployed but becomes relevant the moment we run multiple coordinators (e.g. HA baseLINE, multi-region agent routers, or a quorum-gated treasury). This skill is the reference for *when we scale out* and for reasoning about the distributed properties of what we have (Redis locks, eventual cache consistency, agent coordination via shared state).

## Topics Covered
1. Strong Consistency
2. Eventual Consistency
3. Raft Consensus
4. Leader Election
5. Quorums
6. Clock Skew
7. Consistent Hashing
8. Bloom Filters

Each topic: **Concept → Pattern → Validate → Fix**.

---

## 1. Strong Consistency
**Concept:** A read always returns the latest committed write — linearizable. No stale reads. Expensive: requires coordination (quorum or single leader) on every operation.

**Pattern:** Single leader (primary) handles all reads+writes; followers are read-only replicas (or reads go to primary). For a distributed KV, use a consensus protocol (Raft, topic 3) + quorum reads (topic 5).
```ts
// baseLINE today: all financial writes/reads hit the Postgres PRIMARY → strong consistency
const balance = await primaryPrisma.balances.findUnique({ where: { userId } });
```

**Validate:** After a write commits, every subsequent read (including from any replica that is allowed to serve it) returns the written value. No reader ever observes a value older than one it previously saw.

**Fix:** If you need strong consistency across replicas, route owner/balance reads to primary (see `pcmedicalist-database-deep-dive` topic 20). Don't serve financial reads from a lagging replica. For multi-node, adopt Raft + quorum reads.

---

## 2. Eventual Consistency
**Concept:** Writes propagate to all replicas eventually; a read may briefly return stale data. Higher availability + lower latency, but you must handle temporary inconsistency (idempotency, conflict resolution).

**Pattern:** Use for non-critical, read-heavy data: agent-state caches, Creative Studio asset metadata, public leaderboards. Pair with:
- **Idempotent consumers** (unique constraint, `pcmedicalist-idempotency`).
- **Version vectors / timestamps** to detect staleness.
- **Read-repair** or **hinted handoff** on read.
```ts
// acceptable: a public creator's tip-count may lag 1–2s behind the primary
const count = await replicaPrisma.creators.findUnique({ where: { id }, select: { tipCount: true } });
```

**Validate:** After a write, within the SLA window (e.g. 5s) all replicas converge to the same value. A reader that saw v2 never later sees v1 (monotonic reads via a read timestamp).

**Fix:** Don't use eventual consistency for balances/authz. If you see permanent divergence, check replication lag, a stuck WAL slot, or a crashed relay. Add a convergence monitor (compare replica checksum vs primary periodically).

---

## 3. Raft Consensus
**Concept:** A replicated state machine protocol. One node is leader; it appends entries to a replicated log; a majority (quorum) acknowledges before commit. Survives minority failure. Used by etcd, Consul, TiKV.

**Pattern (when we run HA coordination):**
- Cluster of **odd size** (3, 5) so a majority exists after failure.
- Leader handles all writes; followers replicate the log.
- Election: followers time out → become candidates → request votes → majority → leader. Term numbers prevent old leaders.
- Use a library (e.g. `nativism/raft`, `hashicorp/raft`), don't hand-roll.

```
Term 1: Node A leader
  client write -> A appends to log -> replicates to B,C
  A commits when 2/3 (quorum) ack -> applies to state machine -> replies
```

**Validate:** Kill the leader → within election timeout a new leader is elected (no permanent unavailability). A write acknowledged by the old leader is present on the new leader (log preserved). Split-brain impossible (only one leader per term with majority).

**Fix:** If you see two leaders, check that nodes can't form two majorities (never deploy an even-sized cluster across two AZs without a tie-breaker). Use a battle-tested library. Store the Raft log on durable storage (not tmpfs).

---

## 4. Leader Election
**Concept:** Among N coordinators, exactly one should act as leader (do the work); others stand by. Prevents duplicate work and split-brain.

**Pattern (Redis-based, our current tooling):**
```ts
const lock = await redis.set('baseline:leader', myId, 'PX', 10000, 'NX');
if (lock) {
  // I am leader for 10s; refresh before expiry (lease)
  scheduleRenewal();
} else {
  // someone else is leader; back off
}
```
For true HA, prefer Raft (topic 3) or a coordination service (etcd/ZooKeeper) over a single Redis lock, which has failover gaps. See `pcmedicalist-leader-election` for the dedicated skill.

**Validate:** Start 3 instances → exactly one acquires the leader lock. Kill the leader → within the lease TTL another instance becomes leader. Never two active leaders simultaneously (assert via a shared counter incremented only by the leader).

**Fix:** If two leaders appeared, the lock wasn't exclusive (missing `NX`) or the lease expired while the leader was GC-paused (use fencing tokens / shorter TTLs + active renewal). Add a fencing token so stale leaders' writes are rejected.

---

## 5. Quorums
**Concept:** A majority (⌊N/2⌋+1) of nodes must agree for an operation to commit. Guarantees that any two quorums overlap → no conflicting commits. Balances consistency vs availability.

**Pattern:**
- **Write quorum (W):** ack from W nodes before success.
- **Read quorum (R):** read from R nodes, take the latest (by version).
- **Rule:** `W + R > N` → reads always see latest write (strong consistency).
```
N=5, W=3, R=3  => 3+3=6 > 5  => linearizable
N=5, W=2, R=2  => 2+2=4 < 5  => eventual (stale possible)
```
For baseLINE's future multi-region treasury signing, require a quorum of signers (M-of-N) before broadcasting an on-chain tx.

**Validate:** With N=5 and W=3, kill 2 nodes → writes still succeed (3 alive). Kill 3 → writes block (correctly, no split-brain). A read quorum never returns a value older than a committed write.

**Fix:** Size N odd. Set W,R so `W+R>N` for strong, `W+R<=N` for eventual. Monitor quorum availability; alert when fewer than W nodes are healthy.

---

## 6. Clock Skew
**Concept:** Nodes' clocks drift. If you order events by local wall-clock timestamps, skew causes inverted ordering (a "later" event appears before an "earlier" one). Naive TTL/lease logic breaks.

**Pattern:**
- **Never trust local clocks for ordering.** Use logical clocks (Lamport/vector) or a monotonic counter for causal order.
- **For leases/TTLs:** rely on a central authority (Redis `PEXPIRE`) not each node's `Date.now()`.
- **If you must use time:** use NTP/PTP to bound skew, and use **interval** comparison with a margin, not exact equality.
- **Hybrid Logical Clocks (HLC):** combine wall-clock + logical counter (CockroachDB-style) for distributed ordering with bounded drift.
```ts
// WRONG: ordering by node-local timestamp
events.sort((a,b) => a.localTs - b.localTs);
// RIGHT: use a monotonic sequence or server-assigned HLC
events.sort((a,b) => a.hlc < b.hlc ? -1 : 1);
```

**Validate:** Inject a 5s clock jump on one node → no event is ordered incorrectly; no lease logic double-grants; no duplicate processing due to timestamp collision.

**Fix:** Replace wall-clock ordering with logical/HLC clocks. Run NTP; alert on skew > 100ms between nodes. For lease expiry, use Redis-side expiry, not client-side timers. See `pcmedicalist-clock-skew` if that skill exists in your deployment.

---

## 7. Consistent Hashing
**Concept:** A hash-ring distributes keys across nodes so that adding/removing one node moves only `1/N` of keys (not rehashing everything). Critical for caches, sharded stores, and rendezvous routing.

**Pattern:**
```ts
// Rendezvous / HRW hashing — client-side, no ring state to manage
function nodeFor(key: string, nodes: string[]): string {
  return nodes.map(n => ({ n, h: hash(`${key}::${n}`) }))
              .sort((a,b) => b.h - a.h)[0].n;   // highest hash wins
}
// Adding a node changes the winner for only a fraction of keys
```
For cache rings, add **virtual nodes** (replicas of each physical node on the ring) to balance load when nodes are few.

**Validate:** With N nodes, remove one → only ~1/N of keys remap to a different node (verify by hashing all keys before/after and counting changes). Load distribution across nodes is roughly even (virtual nodes fix hot spots).

**Fix:** If you're using `hash(key) % N` (modulo), switch to consistent hashing — modulo reassigns ALL keys when N changes (cache stampede, topic 97 in resilience). Use virtual nodes for balance. See `pcmedicalist-distributed-locks` for lock-sharding via the same idea.

---

## 8. Bloom Filters
**Concept:** A space-efficient probabilistic set membership test. Answers "definitely NOT in set" or "possibly in set" with zero false negatives, tunable false-positive rate. Great for avoiding expensive lookups (cache miss guard, duplicate detection, CDC dedupe).

**Pattern (Redis Bloom / client-side):**
```ts
// Guard: only hit the DB if the filter says "possibly present"
if (await bloom.exists(`seen:${txHash}`)) {
  const row = await prisma.tips.findUnique({ where: { txHash } });
  if (row) return alreadyProcessed;   // filter said possibly; confirm
}
// on first see: bloom.add(`seen:${txHash}`)
```
Parameters: `m` bits, `k` hash functions. Error rate `p ≈ (1 - e^{-kn/m})^k`. Size for your expected `n` and target `p` (e.g. n=10M, p=0.01 → ~ 14.4 bits/element).

**Validate:** A known-present key is never reported absent (no false negatives). A key never inserted has ≤ p chance of testing present. A membership check is O(k) and uses KB, not MB.

**Fix:** If you're doing a full-table scan or a heavy cache lookup to check "have I seen this?", add a Bloom filter first. Tune `m,k` for your `n,p`. For persistence, use RedisBloom or a serialized bit-array. Note: filters are not a substitute for a unique constraint (topic 13 in database-deep-dive) — they're a cheap first gate.

---

## Validation Checklist
- [ ] Consistency model chosen per data class (strong for $/authz, eventual for caches) (1–2)
- [ ] If HA coordination: odd-sized cluster + Raft library, not hand-rolled (3)
- [ ] Exactly-one leader invariant holds under kill tests (4)
- [ ] Quorum sizes set; `W+R>N` where strong required (5)
- [ ] No wall-clock ordering; logical/HLC clocks used (6)
- [ ] Key distribution uses consistent hashing, not modulo (7)
- [ ] Expensive "seen?" checks gated by a Bloom filter (8)

## Fix Recipes
- **"Two leaders elected"** → topics 3–4: ensure odd cluster, fencing tokens, Raft not ad-hoc lock.
- **"Reads return old data"** → topics 1, 5: route to primary / raise R so W+R>N.
- **"Adding a node flushed the whole cache"** → topic 7: consistent hashing + virtual nodes.
- **"Clock drift broke ordering"** → topic 6: logical/HLC clocks, NTP, server-side TTLs.
- **"Duplicate event processed"** → topic 8 + idempotency: Bloom gate + unique constraint.

## Cross-References
- `pcmedicalist-cap-theorem` — why you trade C vs A
- `pcmedicalist-eventual-consistency` — topic 2 deeper
- `pcmedicalist-leader-election` — topic 4 deeper
- `pcmedicalist-network-partitions` — split-brain, minority survival (3,5)
- `pcmedicalist-distributed-locks` — topic 4 lock-based coordination, topic 7 sharding
- `pcmedicalist-distributed-transactions` / `pcmedicalist-saga-pattern` — multi-node commit
- `pcmedicalist-idempotency` — topic 2, 8 dedupe
- `pcmedicalist-race-conditions` — topic 6 ordering hazards
- `pcmedicalist-cache-invalidation` — topic 2 cache convergence
- `pcmedicalist-database-deep-dive` — topic 8 Bloom + unique (CDC dedupe), topic 19–20 replicas
