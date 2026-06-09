module cursor_chain_liveness {
  type Writer = str
  type Checksum = str

  type Delta = {
    seq: int,
    epoch: int,
    writer: Writer,
    prevChecksum: Checksum,
    checksum: Checksum,
    payloadFingerprint: Checksum,
    endPageCount: int,
  }

  pure val stableEpoch: int = 0
  pure val stableWriter: Writer = "A"
  pure val baseChecksum: Checksum = "base0"
  pure val targetSeq: int = 3

  pure val delta1: Delta = {
    seq: 1,
    epoch: stableEpoch,
    writer: stableWriter,
    prevChecksum: baseChecksum,
    checksum: "d1",
    payloadFingerprint: "p1",
    endPageCount: 1,
  }

  pure val delta2: Delta = {
    seq: 2,
    epoch: stableEpoch,
    writer: stableWriter,
    prevChecksum: "d1",
    checksum: "d2",
    payloadFingerprint: "p2",
    endPageCount: 2,
  }

  pure val delta3: Delta = {
    seq: 3,
    epoch: stableEpoch,
    writer: stableWriter,
    prevChecksum: "d2",
    checksum: "d3",
    payloadFingerprint: "p3",
    endPageCount: 1,
  }

  pure val stablePrefix: Set[Delta] = Set(delta1, delta2, delta3)

  var deltas: Set[Delta]
  var followerSeq: int
  var followerEpoch: int
  var followerWriter: Writer
  var chainAnchor: Checksum
  var pageCount: int
  var applied: Set[Delta]
  var fatal: bool

  action init = all {
    deltas' = stablePrefix,
    followerSeq' = 0,
    followerEpoch' = stableEpoch,
    followerWriter' = stableWriter,
    chainAnchor' = baseChecksum,
    pageCount' = 0,
    applied' = Set(),
    fatal' = false,
  }

  action pollNoop = all {
    deltas' = deltas,
    followerSeq' = followerSeq,
    followerEpoch' = followerEpoch,
    followerWriter' = followerWriter,
    chainAnchor' = chainAnchor,
    pageCount' = pageCount,
    applied' = applied,
    fatal' = fatal,
  }

  action applyNext = {
    nondet d = oneOf(deltas.filter(d =>
      d.seq == followerSeq + 1
        and d.epoch == followerEpoch
        and d.writer == followerWriter
        and d.prevChecksum == chainAnchor
    ))
    all {
      not(fatal),
      followerSeq < targetSeq,
      deltas' = deltas,
      followerSeq' = d.seq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
    }
  }

  /// Stable-prefix follower polling relation for this liveness slice: a poll
  /// can observe no new progress, or it can apply the next available delta.
  /// Root publishing, writer churn, object-store discovery, and fatal
  /// equivocation are modeled in `cursor_chain.qnt`, not in this temporal slice.
  action step = any {
    applyNext,
    pollNoop,
  }

  def appliedHasSeq(seq: int): bool =
    applied.exists(d => d.seq == seq)

  def stableEnvironment: bool =
    deltas == stablePrefix
      and followerEpoch == stableEpoch
      and followerWriter == stableWriter
      and not(fatal)

  def prefixCaughtUp: bool =
    followerSeq == targetSeq

  def stablePrefixAvailable: bool =
    stableEnvironment
      and deltas.contains(delta1)
      and deltas.contains(delta2)
      and deltas.contains(delta3)

  def contiguousApplied: bool =
    1.to(targetSeq).forall(seq => seq > followerSeq or appliedHasSeq(seq))

  def appliedMatchesFence: bool =
    applied.forall(d => d.epoch == followerEpoch and d.writer == followerWriter)

  def appliedChainsFromBase: bool =
    applied.forall(d =>
      if (d.seq == 1) {
        d.prevChecksum == baseChecksum
      } else {
        applied.exists(prev =>
          prev.seq == d.seq - 1 and d.prevChecksum == prev.checksum
        )
      }
    )

  def safety: bool = and {
    followerSeq >= 0,
    followerSeq <= targetSeq,
    contiguousApplied,
    appliedMatchesFence,
    appliedChainsFromBase,
  }

  val livenessVars = (
    deltas,
    followerSeq,
    followerEpoch,
    followerWriter,
    chainAnchor,
    pageCount,
    applied,
    fatal,
  )

  /// If a valid prefix remains available in a stable environment, weak
  /// fairness of `applyNext` rules out polling forever without applying it.
  temporal fairStablePrefixProgress =
    applyNext.weakFair(livenessVars)
      implies (stablePrefixAvailable leadsTo prefixCaughtUp)

  /// Expected to fail: without fairness, a follower can poll forever without
  /// taking `applyNext`, even though the prefix remains available.
  temporal badUnfairPollingProgress =
    stablePrefixAvailable leadsTo prefixCaughtUp
}
