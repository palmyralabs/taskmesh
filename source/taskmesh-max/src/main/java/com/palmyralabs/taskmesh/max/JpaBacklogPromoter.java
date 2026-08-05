package com.palmyralabs.taskmesh.max;

import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.max.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Throttled {@link BacklogPromoter} — {@link #onEnqueue} is a no-op (this
 * tier's {@code enqueue()} only ever writes {@code tm_task}), so a large
 * batch enqueued at once leaves {@code tm_in_flight} untouched until
 * {@link #promote} — called periodically by the engine, per {@code
 * taskType} — creates hot rows for the oldest still-backlogged tasks.
 *
 * <h4>The in-flight window</h4>
 * Promotion regulates to a <b>level</b>, not to a rate: each cycle measures how
 * much claimable work this node currently has and tops it back up towards
 * {@code highWatermark}, but only once it has fallen below {@code lowWatermark}
 * — plain hysteresis, so a busy queue is not re-topped by a handful of rows every
 * few seconds, and each promotion transaction does enough work to be worth its
 * round trip.
 *
 * <p>A rate-only throttle (promote N per cycle unconditionally, never looking at
 * the level) does <em>not</em> bound anything: if the drain rate falls below the
 * promotion rate, {@code tm_in_flight} still grows without limit, just more
 * slowly than the enqueue rate did. Reading the level is what makes the bound real.
 *
 * <h4>What "the level" means — claimable work, not a row count</h4>
 * The measurement is {@link PartitionedTaskLedger#countClaimable}: an unpaginated
 * {@code COUNT} carrying {@code findClaimable}'s eligibility exactly — <b>what the
 * poller would actually pick up</b>. A plain {@code COUNT(*)} over
 * {@code tm_in_flight} would be the wrong number in two ways:
 *
 * <ul>
 *   <li>A row inside its {@code next_retry_at} backoff is <em>not</em> work
 *       anyone can pick up. Counting it lets a queue full of backing-off retries
 *       read as a full window while every worker sits idle — promotion starves
 *       exactly when the failure rate is highest.</li>
 *   <li>Claimability is <b>node-relative</b>. A row reserved to another node, or
 *       held inside its affinity window, is not this node's to claim. The four
 *       eligibility windows (reservation/reclaim/affinity/claim-timeout) decide
 *       that, and only the ledger knows them.</li>
 * </ul>
 *
 * <p>The count is across <b>every task type</b>, not the one {@code promote} was
 * called for: the window is a property of this node's capacity, and scoping it per
 * type would multiply the configured window by however many types are registered.
 * One consequence to know about — the engine calls {@code promote} once per task
 * type per cycle, so whichever type is visited first fills the shared window and
 * the rest see it already full. Over successive cycles that evens out as each type's
 * rows drain, but a single cycle is first-come, in registration order.
 *
 * <p>The consequence, stated plainly: the window bounds <b>available work</b>, not
 * table size. {@code tm_in_flight} can hold more rows than {@code highWatermark} —
 * backed-off retries and other nodes' reservations sit on top of the window. That is
 * the right trade (workers stay fed), but it means this is not a hard cap on the
 * table, and a deployment that needs one has to bound the retry budget too.
 *
 * <h4>Sizing the window against the drain rate</h4>
 * These are not free parameters. A refill leaves the level at {@code highWatermark},
 * and the next cycle is up to one promotion interval {@code T} away, so for a node
 * draining {@code R} tasks/second:
 *
 * <pre>
 *   lowWatermark  &gt;= R * T        // trigger while a full interval's work is still in hand
 *   highWatermark &gt;= low + R * T  // a refill adds an interval's worth
 *   limit         &gt;= high - low   // ...and one cycle can actually deliver it
 * </pre>
 *
 * Undersize the low mark and the window empties before the next cycle refills it:
 * every worker idles in front of a backlog of millions and throughput collapses to
 * {@code (high - low) / T}, however many workers are configured. Nothing errors —
 * the queue is simply slow — which is why {@link #promote} logs when {@code limit}
 * cannot cover the window.
 *
 * <p>The engine's {@code limit} argument still caps a single cycle: the interface
 * contract is "up to {@code limit}", and honouring it keeps one promotion
 * transaction bounded no matter how deep the deficit is.
 *
 * <p>Every node runs its own promoter and they are not coordinated, and each
 * measures its <em>own</em> claimable level. Freshly promoted rows are reserved to
 * the promoting node, so they count towards that node's window and not its peers' —
 * meaning cluster-wide {@code tm_in_flight} tends towards N x {@code highWatermark}
 * for N nodes, not {@code highWatermark}. Size the watermarks per node. No leader
 * election or advisory lock is used to prevent it: that would be the cross-node
 * coordinator the design deliberately does not have, and the per-row
 * {@code findById} guard below already means an overshoot duplicates no rows.
 */
@Slf4j
public class JpaBacklogPromoter implements BacklogPromoter {

    private final TmTaskRepository taskRepository;
    private final TmInFlightRepository inFlightRepository;
    private final TransactionTemplate transactionTemplate;
    /**
     * Measures the window. Typed as the concrete ledger, not {@link TaskLedger}, because
     * {@code countClaimable} is where the four eligibility windows live and only this tier's
     * ledger has them — putting it on the {@code -api} interface would force every tier to
     * implement a promotion concept none of them use.
     */
    private final PartitionedTaskLedger taskLedger;
    private final String nodeId;
    /** Top back up only once the claimable level has fallen below this. */
    private final int lowWatermark;
    /** Never promote past this level in one cycle. */
    private final int highWatermark;
    /**
     * When non-null, the window is derived from observed consumption each cycle and the two
     * watermarks above act only as its floor and ceiling. Null keeps them fixed.
     */
    private final InFlightWindow window;
    private final AtomicBoolean undersizedBatchWarned = new AtomicBoolean();

    /**
     * @throws IllegalArgumentException if the watermarks are not a usable window —
     *         a zero/negative low mark would promote nothing ever, and an inverted
     *         pair would promote a negative deficit, both of which present as a
     *         queue that silently stops draining.
     */
    public JpaBacklogPromoter(TmTaskRepository taskRepository, TmInFlightRepository inFlightRepository,
                               TransactionTemplate transactionTemplate, PartitionedTaskLedger taskLedger, String nodeId,
                               int lowWatermark, int highWatermark) {
        this(taskRepository, inFlightRepository, transactionTemplate, taskLedger, nodeId,
                lowWatermark, highWatermark, false, 5, 95);
    }

    /**
     * @param adaptiveWindow when true the window is re-derived every cycle from observed
     *                       consumption ({@link InFlightWindow}) and the two watermarks become its
     *                       floor and ceiling rather than the window itself
     * @param promotionIntervalSeconds the promoter's own schedule, which is what turns an observed
     *                       consumption <i>rate</i> into rows-per-cycle
     */
    public JpaBacklogPromoter(TmTaskRepository taskRepository, TmInFlightRepository inFlightRepository,
                               TransactionTemplate transactionTemplate, PartitionedTaskLedger taskLedger, String nodeId,
                               int lowWatermark, int highWatermark,
                               boolean adaptiveWindow, long promotionIntervalSeconds, int regularQueueCapacity) {
        if (lowWatermark <= 0) {
            throw new IllegalArgumentException("lowWatermark must be positive, was " + lowWatermark);
        }
        if (highWatermark < lowWatermark) {
            throw new IllegalArgumentException(
                    "highWatermark (" + highWatermark + ") must be >= lowWatermark (" + lowWatermark + ")");
        }
        this.taskRepository = taskRepository;
        this.inFlightRepository = inFlightRepository;
        this.transactionTemplate = transactionTemplate;
        this.taskLedger = Objects.requireNonNull(taskLedger, "taskLedger is required to measure the in-flight window");
        this.nodeId = nodeId;
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
        this.window = adaptiveWindow
                ? new InFlightWindow(lowWatermark, highWatermark, promotionIntervalSeconds, regularQueueCapacity, nodeId)
                : null;
    }

    @Override
    public void onEnqueue(TaskRef taskRef, String nodeId) {
        // no-op — hot-row creation happens later, in bounded batches, from promote().
    }

    @Override
    public int promote(String taskType, Instant now, int limit) {
        // Silence is a bug: an undersized batch cannot refill the window in one cycle, which
        // presents only as a queue that drains slowly with idle workers. Warned once, not per
        // cycle — this is a configuration fact, not an event.
        if (limit < highWatermark - lowWatermark && undersizedBatchWarned.compareAndSet(false, true)) {
            log.warn("taskmesh.backlog-batch-size ({}) is smaller than the in-flight window it has to refill ({}..{}, "
                    + "a deficit of {}). Promotion will need {} cycles to refill, so the window can empty between "
                    + "cycles and workers will idle in front of the backlog. Raise backlog-batch-size to at least {}.",
                    limit, lowWatermark, highWatermark, highWatermark - lowWatermark,
                    (int) Math.ceil((double) (highWatermark - lowWatermark) / Math.max(1, limit)),
                    highWatermark - lowWatermark);
        }
        try {
            return promoteBatch(taskType, now, limit);
        } catch (DataIntegrityViolationException lostRace) {
            // Every node promotes on its own timer, unsynchronised, and they all scan the same
            // oldest-unpromoted rows. The per-row findById guard cannot see a peer's uncommitted
            // insert, so two nodes reaching the same row in the same instant both INSERT and the
            // loser takes a PK violation on tm_in_flight — which poisons its whole transaction.
            //
            // Losing is not an error, exactly as in TaskLedger.enqueue's duplicate-race swallow:
            // the winner's hot rows are precisely what this call wanted to create. The rollback
            // discards this cycle's batch and the next cycle sees the rows already stamped and
            // moves on to the next slice. Swallowed here rather than left to propagate because
            // promote() runs on a scheduler, where an escaping exception cancels all future runs.
            log.debug("Promotion of {} lost the race with a peer node; the winner's rows stand", taskType, lostRace);
            return 0;
        }
    }

    private int promoteBatch(String taskType, Instant rawNow, int limit) {
        // Truncated to microseconds because promoted_at doubles as this call's marker for
        // findPromotedAt, and the column is TIMESTAMP(6): an Instant's nanoseconds are discarded on
        // write, so the value read back would never equal the full-precision value passed in, and
        // the slice would come back empty every single time.
        Instant now = rawNow.truncatedTo(ChronoUnit.MICROS);
        Integer promoted = transactionTemplate.execute(status -> {
            // Measure first: the deficit, not `limit`, governs how many rows this cycle wants.
            // countClaimable applies pullPending's eligibility exactly — node-scoped and
            // backoff-gated — so a row this node cannot actually pick up right now does not count
            // towards its window. An unpaginated COUNT, not a page size: the level has to be the
            // real one, and a count capped at the window could never report an overshoot.
            long claimable = taskLedger.countClaimable(now);
            int low = lowWatermark;
            int high = highWatermark;
            if (window != null) {
                // Adaptive: size the window from what this node is actually consuming, and let the
                // configured watermarks act as floor and ceiling. See InFlightWindow.
                window.observe(claimable, System.nanoTime());
                low = window.low();
                high = window.high();
            }
            if (claimable >= low) {
                return 0;   // still inside the window — nothing to do this cycle
            }
            int batch = (int) Math.min(limit, high - claimable);
            if (batch <= 0) {
                return 0;
            }
            List<TmTaskEntity> candidates = taskRepository.findUnpromoted(taskType, PageRequest.of(0, batch));
            if (candidates.isEmpty()) {
                return 0;
            }

            // Claim the slice BEFORE creating any hot row. Every node scans the same oldest rows,
            // so without an atomic gate here two promoters both act on the same slice and the loser
            // dies on a PK violation. Stamping first — guarded by promoted_at IS NULL — makes this
            // the same one-winner UPDATE the claim path uses: the loser's UPDATE touches 0 rows and
            // it takes the next slice on its following cycle, having written nothing.
            List<String> candidateIds = candidates.stream().map(TmTaskEntity::getTaskId).toList();
            int won = taskRepository.stampPromoted(taskType, candidateIds, now);
            if (won == 0) {
                log.debug("Promotion of {} lost the whole slice to a peer node; taking the next one next cycle", taskType);
                return 0;
            }

            // One set-based INSERT ... SELECT for the whole slice, scoped to the rows this call
            // stamped — a partially-won slice must not create hot rows for a peer's share. This
            // used to be a per-row loop costing ~3N+4 statements; it is now exactly one, so a
            // 500-row batch no longer floods the same connections the drain is using.
            //
            // Rows are reserved to THIS node deliberately: the window is measured per node
            // (countClaimable is node-scoped), so each node promotes and owns its own working set.
            // That is what gives every node its own low..high of available work, and means a node
            // only ever claims rows reserved to itself — no cross-node claim contention at all.
            // The reservation window still lets a peer steal the set if this node dies.
            int inserted = inFlightRepository.insertPromotedSlice(taskType, candidateIds, now, nodeId,
                    TmTaskEntities.UNBOUNDED_MAX_RETRIES);
            if (window != null) {
                // The window has to know what we added, or the next cycle reads our own promotion
                // as consumption and the rate estimate runs away.
                window.promoted(inserted);
            }
            return inserted;
        });
        return promoted == null ? 0 : promoted;
    }
}
