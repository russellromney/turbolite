module cursor_chain {
  type Writer = str
  type Checksum = str

  type Base = {
    seq: int,
    epoch: int,
    writer: Writer,
    checksum: Checksum,
    endPageCount: int,
  }

  type Delta = {
    seq: int,
    epoch: int,
    writer: Writer,
    prevChecksum: Checksum,
    checksum: Checksum,
    payloadFingerprint: Checksum,
    endPageCount: int,
  }

  pure val writers: Set[Writer] = Set("A", "B")
  pure val epochs: Set[int] = Set(0, 1)
  pure val seqs: Set[int] = Set(1, 2, 3)
  pure val pageCounts: Set[int] = Set(0, 1, 2)
  pure val checksums: Set[Checksum] = Set("base0", "base1", "d1", "d2", "d3", "bad")
  pure val payloadFingerprints: Set[Checksum] = Set("payload", "otherPayload")

  pure val initialBase: Base = {
    seq: 0,
    epoch: 0,
    writer: "A",
    checksum: "base0",
    endPageCount: 0,
  }

  pure val promotedBaseChecksum: Checksum = "base1"

  pure val progressDelta1: Delta = {
    seq: 1,
    epoch: 0,
    writer: "A",
    prevChecksum: "base0",
    checksum: "d1",
    payloadFingerprint: "p1",
    endPageCount: 1,
  }

  pure val progressDelta2: Delta = {
    seq: 2,
    epoch: 0,
    writer: "A",
    prevChecksum: "d1",
    checksum: "d2",
    payloadFingerprint: "p2",
    endPageCount: 2,
  }

  pure val progressDelta3: Delta = {
    seq: 3,
    epoch: 0,
    writer: "A",
    prevChecksum: "d2",
    checksum: "d3",
    payloadFingerprint: "p3",
    endPageCount: 1,
  }

  pure val deltaUniverse: Set[Delta] =
    tuples(seqs, epochs, writers, checksums, checksums, payloadFingerprints, pageCounts)
      .map(((seq, epoch, writer, prevChecksum, checksum, payloadFingerprint, endPageCount)) => {
        seq: seq,
        epoch: epoch,
        writer: writer,
        prevChecksum: prevChecksum,
        checksum: checksum,
        payloadFingerprint: payloadFingerprint,
        endPageCount: endPageCount,
      })

  var root: Base
  var leaseEpoch: int
  var leaseHolder: Writer
  var deltas: Set[Delta]
  var followerBaseSeq: int
  var followerBaseChecksum: Checksum
  var followerSeq: int
  var followerEpoch: int
  var followerWriter: Writer
  var chainAnchor: Checksum
  var pageCount: int
  var applied: Set[Delta]
  var fatal: bool
  var promoted: bool
  var promotedFromSeq: int
  var promotedFromPageCount: int
  var promotedFromApplied: Set[Delta]
  var progressSeeded: bool
  var progressTicks: int

  action init = all {
    root' = initialBase,
    leaseEpoch' = 0,
    leaseHolder' = "A",
    deltas' = Set(),
    followerBaseSeq' = initialBase.seq,
    followerBaseChecksum' = initialBase.checksum,
    followerSeq' = initialBase.seq,
    followerEpoch' = initialBase.epoch,
    followerWriter' = initialBase.writer,
    chainAnchor' = initialBase.checksum,
    pageCount' = initialBase.endPageCount,
    applied' = Set(),
    fatal' = false,
    promoted' = false,
    promotedFromSeq' = initialBase.seq,
    promotedFromPageCount' = initialBase.endPageCount,
    promotedFromApplied' = Set(),
    progressSeeded' = false,
    progressTicks' = 0,
  }

  pure def sameCandidateSlot(a: Delta, b: Delta): bool =
    a.seq == b.seq and a.epoch == b.epoch and a.writer == b.writer

  pure def divergentCandidate(a: Delta, b: Delta): bool =
    sameCandidateSlot(a, b)
      and (
        a.prevChecksum != b.prevChecksum
          or a.checksum != b.checksum
          or a.payloadFingerprint != b.payloadFingerprint
          or a.endPageCount != b.endPageCount
      )

  /// `checksum` abstracts BLAKE3 over the production delta envelope.
  /// `payloadFingerprint` stands for compacted fields this model does not
  /// expand, including changed page bytes, commit boundary, and idempotency key.
  pure def samePayloadShape(a: Delta, b: Delta): bool =
    a.seq == b.seq
      and a.epoch == b.epoch
      and a.writer == b.writer
      and a.prevChecksum == b.prevChecksum
      and a.payloadFingerprint == b.payloadFingerprint
      and a.endPageCount == b.endPageCount

  pure def hasEquivocation(candidates: Set[Delta]): bool =
    candidates.exists(a => candidates.exists(b => divergentCandidate(a, b)))

  pure def hasChecksumCollision(candidates: Set[Delta]): bool =
    candidates.exists(a =>
      candidates.exists(b =>
        a.checksum == b.checksum and not(samePayloadShape(a, b))
      )
    )

  pure def candidatesWellFormed(candidates: Set[Delta]): bool =
    not(hasEquivocation(candidates)) and not(hasChecksumCollision(candidates))

  action noStateChange = all {
    root' = root,
    leaseEpoch' = leaseEpoch,
    leaseHolder' = leaseHolder,
    deltas' = deltas,
    followerBaseSeq' = followerBaseSeq,
    followerBaseChecksum' = followerBaseChecksum,
    followerSeq' = followerSeq,
    followerEpoch' = followerEpoch,
    followerWriter' = followerWriter,
    chainAnchor' = chainAnchor,
    pageCount' = pageCount,
    applied' = applied,
    fatal' = fatal,
    promoted' = promoted,
    promotedFromSeq' = promotedFromSeq,
    promotedFromPageCount' = promotedFromPageCount,
    promotedFromApplied' = promotedFromApplied,
    progressSeeded' = progressSeeded,
    progressTicks' = progressTicks,
  }

  action appendDelta = {
    nondet d = oneOf(deltaUniverse)
    all {
      candidatesWellFormed(deltas.union(Set(d))),
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(d)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = chainAnchor,
      pageCount' = pageCount,
      applied' = applied,
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  action recordEquivocation = {
    nondet a = oneOf(deltaUniverse)
    nondet b = oneOf(deltaUniverse.filter(b => divergentCandidate(a, b)))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(a, b)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = chainAnchor,
      pageCount' = pageCount,
      applied' = applied,
      fatal' = true,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  action applyNext = {
    nondet d = oneOf(deltas)
    all {
      not(fatal),
      d.seq == followerSeq + 1,
      d.epoch == followerEpoch,
      d.writer == followerWriter,
      d.prevChecksum == chainAnchor,

      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas,
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  action promote = {
    nondet newWriter = oneOf(writers)
    all {
      not(fatal),
      not(promoted),
      root' = {
        seq: followerSeq,
        epoch: leaseEpoch + 1,
        writer: newWriter,
        checksum: promotedBaseChecksum,
        endPageCount: pageCount,
      },
      leaseEpoch' = leaseEpoch + 1,
      leaseHolder' = newWriter,
      deltas' = deltas,
      followerBaseSeq' = followerSeq,
      followerBaseChecksum' = promotedBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = leaseEpoch + 1,
      followerWriter' = newWriter,
      chainAnchor' = promotedBaseChecksum,
      pageCount' = pageCount,
      applied' = Set(),
      fatal' = fatal,
      promoted' = true,
      promotedFromSeq' = followerSeq,
      promotedFromPageCount' = pageCount,
      promotedFromApplied' = applied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  action step = any {
    appendDelta,
    recordEquivocation,
    applyNext,
    promote,
    noStateChange,
  }

  action seedProgressChain = all {
    not(progressSeeded),
    not(fatal),
    not(promoted),
    root == initialBase,
    followerSeq == initialBase.seq,
    followerEpoch == initialBase.epoch,
    followerWriter == initialBase.writer,
    chainAnchor == initialBase.checksum,

    root' = root,
    leaseEpoch' = leaseEpoch,
    leaseHolder' = leaseHolder,
    deltas' = Set(progressDelta1, progressDelta2, progressDelta3),
    followerBaseSeq' = followerBaseSeq,
    followerBaseChecksum' = followerBaseChecksum,
    followerSeq' = followerSeq,
    followerEpoch' = followerEpoch,
    followerWriter' = followerWriter,
    chainAnchor' = chainAnchor,
    pageCount' = pageCount,
    applied' = applied,
    fatal' = fatal,
    promoted' = promoted,
    promotedFromSeq' = promotedFromSeq,
    promotedFromPageCount' = promotedFromPageCount,
    promotedFromApplied' = promotedFromApplied,
    progressSeeded' = true,
    progressTicks' = 0,
  }

  action progressApplyNext = {
    nondet d = oneOf(deltas.filter(d =>
      d.seq == followerSeq + 1
        and d.epoch == followerEpoch
        and d.writer == followerWriter
        and d.prevChecksum == chainAnchor
    ))
    all {
      progressSeeded,
      not(fatal),
      followerSeq < 3,

      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas,
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks + 1,
    }
  }

  action progressDone = all {
    progressSeeded,
    followerSeq == 3,

    root' = root,
    leaseEpoch' = leaseEpoch,
    leaseHolder' = leaseHolder,
    deltas' = deltas,
    followerBaseSeq' = followerBaseSeq,
    followerBaseChecksum' = followerBaseChecksum,
    followerSeq' = followerSeq,
    followerEpoch' = followerEpoch,
    followerWriter' = followerWriter,
    chainAnchor' = chainAnchor,
    pageCount' = pageCount,
    applied' = applied,
    fatal' = fatal,
    promoted' = promoted,
    promotedFromSeq' = promotedFromSeq,
    promotedFromPageCount' = promotedFromPageCount,
    promotedFromApplied' = promotedFromApplied,
    progressSeeded' = progressSeeded,
    progressTicks' = progressTicks,
  }

  action progressStep = any {
    seedProgressChain,
    progressApplyNext,
    progressDone,
  }

  def appliedHasSeq(seq: int): bool =
    applied.exists(d => d.seq == seq)

  def oneAppliedPerSeq: bool =
    applied.forall(a => applied.forall(b => a.seq != b.seq or a == b))

  def contiguousApplied: bool =
    seqs.forall(seq =>
      seq <= followerBaseSeq
        or seq > followerSeq
        or appliedHasSeq(seq)
    )

  def appliedMatchesFence: bool =
    applied.forall(d => d.epoch == followerEpoch and d.writer == followerWriter)

  def appliedChainsFromBase: bool =
    applied.forall(d =>
      if (d.seq == followerBaseSeq + 1) {
        d.prevChecksum == followerBaseChecksum
      } else {
        applied.exists(prev =>
          prev.seq == d.seq - 1 and d.prevChecksum == prev.checksum
        )
      }
    )

  def pageCountMatchesCursor: bool =
    if (followerSeq == followerBaseSeq) {
      pageCount == root.endPageCount
    } else {
      applied.exists(d => d.seq == followerSeq and pageCount == d.endPageCount)
    }

  def equivocationIsFatal: bool =
    hasEquivocation(deltas) implies fatal

  def checksumCollisionIsFatal: bool =
    hasChecksumCollision(deltas) implies fatal

  def rootMatchesLease: bool =
    root.epoch == leaseEpoch and root.writer == leaseHolder

  def followerBaseMatchesRoot: bool =
    followerBaseSeq == root.seq and followerBaseChecksum == root.checksum

  def promotedAppliedHasSeq(seq: int): bool =
    promotedFromApplied.exists(d => d.seq == seq)

  def promotedAppliedPrefixExact: bool =
    promoted implies and {
      root.seq == promotedFromSeq,
      root.endPageCount == promotedFromPageCount,
      seqs.forall(seq =>
        seq <= initialBase.seq
          or seq > promotedFromSeq
          or promotedAppliedHasSeq(seq)
      ),
      promotedFromApplied.forall(d =>
        d.seq > initialBase.seq
          and d.seq <= promotedFromSeq
          and d.epoch == initialBase.epoch
          and d.writer == initialBase.writer
      ),
      if (promotedFromSeq == initialBase.seq) {
        promotedFromPageCount == initialBase.endPageCount
      } else {
        promotedFromApplied.exists(d =>
          d.seq == promotedFromSeq and d.endPageCount == promotedFromPageCount
        )
      },
    }

  def boundedProgress: bool =
    not(progressSeeded) or progressTicks < 3 or fatal or followerSeq == 3

  def safety: bool = and {
    rootMatchesLease,
    followerBaseMatchesRoot,
    promotedAppliedPrefixExact,
    followerSeq >= followerBaseSeq,
    followerEpoch == leaseEpoch,
    followerWriter == leaseHolder,
    oneAppliedPerSeq,
    contiguousApplied,
    appliedMatchesFence,
    appliedChainsFromBase,
    pageCountMatchesCursor,
    equivocationIsFatal,
    checksumCollisionIsFatal,
  }

  /// Bad transition: applies a delta from the wrong epoch.
  action badWrongEpochStep = {
    nondet d = oneOf(deltaUniverse.filter(d =>
      d.seq == followerSeq + 1
        and d.epoch != followerEpoch
        and d.writer == followerWriter
        and d.prevChecksum == chainAnchor
    ))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(d)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = d.epoch,
      followerWriter' = followerWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: applies a delta from the wrong writer.
  action badWrongWriterStep = {
    nondet d = oneOf(deltaUniverse.filter(d =>
      d.seq == followerSeq + 1
        and d.epoch == followerEpoch
        and d.writer != followerWriter
        and d.prevChecksum == chainAnchor
    ))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(d)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = followerEpoch,
      followerWriter' = d.writer,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: skips the next contiguous sequence.
  action badSkippedSeqStep = {
    nondet d = oneOf(deltaUniverse.filter(d =>
      d.seq > followerSeq + 1
        and d.epoch == followerEpoch
        and d.writer == followerWriter
    ))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(d)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: accepts a delta with a broken chain link.
  action badPrevChecksumStep = {
    nondet d = oneOf(deltaUniverse.filter(d =>
      d.seq == followerSeq + 1
        and d.epoch == followerEpoch
        and d.writer == followerWriter
        and d.prevChecksum != chainAnchor
    ))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(d)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = applied.union(Set(d)),
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: creates equivocation but forgets to enter fatal mode.
  action badEquivocationStep = {
    nondet a = oneOf(deltaUniverse)
    nondet b = oneOf(deltaUniverse.filter(b => divergentCandidate(a, b)))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(a, b)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = chainAnchor,
      pageCount' = pageCount,
      applied' = applied,
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: admits two different payload shapes with one checksum.
  action badChecksumCollisionStep = {
    nondet a = oneOf(deltaUniverse)
    nondet b = oneOf(deltaUniverse.filter(b =>
      a.checksum == b.checksum and not(samePayloadShape(a, b))
    ))
    all {
      root' = root,
      leaseEpoch' = leaseEpoch,
      leaseHolder' = leaseHolder,
      deltas' = deltas.union(Set(a, b)),
      followerBaseSeq' = followerBaseSeq,
      followerBaseChecksum' = followerBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = followerEpoch,
      followerWriter' = followerWriter,
      chainAnchor' = chainAnchor,
      pageCount' = pageCount,
      applied' = applied,
      fatal' = fatal,
      promoted' = promoted,
      promotedFromSeq' = promotedFromSeq,
      promotedFromPageCount' = promotedFromPageCount,
      promotedFromApplied' = promotedFromApplied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: promotes a base whose cursor skips the applied prefix.
  action badPromotionCursorStep = {
    nondet newWriter = oneOf(writers)
    all {
      not(promoted),
      root' = {
        seq: followerSeq + 1,
        epoch: leaseEpoch + 1,
        writer: newWriter,
        checksum: promotedBaseChecksum,
        endPageCount: pageCount,
      },
      leaseEpoch' = leaseEpoch + 1,
      leaseHolder' = newWriter,
      deltas' = deltas,
      followerBaseSeq' = followerSeq + 1,
      followerBaseChecksum' = promotedBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = leaseEpoch + 1,
      followerWriter' = newWriter,
      chainAnchor' = promotedBaseChecksum,
      pageCount' = pageCount,
      applied' = Set(),
      fatal' = fatal,
      promoted' = true,
      promotedFromSeq' = followerSeq,
      promotedFromPageCount' = pageCount,
      promotedFromApplied' = applied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: applies an old writer's candidate after promotion.
  action badStaleWriterAfterPromotionStep = {
    nondet newWriter = oneOf(writers.filter(writer => writer != initialBase.writer))
    nondet d = oneOf(deltaUniverse.filter(d =>
      d.seq == initialBase.seq + 1
        and d.epoch == initialBase.epoch
        and d.writer == initialBase.writer
        and d.prevChecksum == initialBase.checksum
    ))
    all {
      root' = {
        seq: initialBase.seq,
        epoch: initialBase.epoch + 1,
        writer: newWriter,
        checksum: promotedBaseChecksum,
        endPageCount: initialBase.endPageCount,
      },
      leaseEpoch' = initialBase.epoch + 1,
      leaseHolder' = newWriter,
      deltas' = Set(d),
      followerBaseSeq' = initialBase.seq,
      followerBaseChecksum' = promotedBaseChecksum,
      followerSeq' = d.seq,
      followerEpoch' = initialBase.epoch + 1,
      followerWriter' = newWriter,
      chainAnchor' = d.checksum,
      pageCount' = d.endPageCount,
      applied' = Set(d),
      fatal' = fatal,
      promoted' = true,
      promotedFromSeq' = initialBase.seq,
      promotedFromPageCount' = initialBase.endPageCount,
      promotedFromApplied' = Set(),
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }

  /// Bad transition: promotes a base with the right cursor but wrong page count.
  action badPromotionPageCountStep = {
    nondet newWriter = oneOf(writers)
    nondet wrongPageCount = oneOf(pageCounts.filter(pc => pc != pageCount))
    all {
      not(promoted),
      root' = {
        seq: followerSeq,
        epoch: leaseEpoch + 1,
        writer: newWriter,
        checksum: promotedBaseChecksum,
        endPageCount: wrongPageCount,
      },
      leaseEpoch' = leaseEpoch + 1,
      leaseHolder' = newWriter,
      deltas' = deltas,
      followerBaseSeq' = followerSeq,
      followerBaseChecksum' = promotedBaseChecksum,
      followerSeq' = followerSeq,
      followerEpoch' = leaseEpoch + 1,
      followerWriter' = newWriter,
      chainAnchor' = promotedBaseChecksum,
      pageCount' = pageCount,
      applied' = Set(),
      fatal' = fatal,
      promoted' = true,
      promotedFromSeq' = followerSeq,
      promotedFromPageCount' = pageCount,
      promotedFromApplied' = applied,
      progressSeeded' = progressSeeded,
      progressTicks' = progressTicks,
    }
  }
}
