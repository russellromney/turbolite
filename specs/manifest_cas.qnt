module manifest_cas {
  /// Model of the Turbolite manifest upload compare-and-swap contract.
  ///
  /// `storage.rs::put_manifest` must always read the remote manifest before
  /// uploading. If a remote manifest already exists with version >= the local
  /// manifest version, the upload is rejected. This prevents a slow writer or
  /// a creation-time race from silently clobbering a newer (or contemporaneous)
  /// remote database.
  ///
  /// The model abstracts page groups, checksums, and writers; it only tracks
  /// the monotonic version counter that the CAS guards. Version 0 means "no
  /// manifest published yet".

  type Writer = str

  pure val writers: Set[Writer] = Set("A", "B")
  pure val versions: Set[int] = Set(1, 2, 3, 4)

  /// Local state for each writer: the manifest they are trying to publish.
  var localVersion: Writer -> int
  /// Remote state: the manifest version currently stored in the object store.
  /// 0 means no manifest has been published yet.
  var remoteVersion: int
  /// Maximum remote version ever observed. Used to detect regressions in the
  /// bad-action negative checks; normal CAS-protected uploads are monotone.
  var maxRemoteVersion: int
  /// Successful uploads observed by each writer (0 means not yet published).
  var publishedVersion: Writer -> int

  action init = all {
    localVersion' = writers.mapBy(_ => 1),
    remoteVersion' = 0,
    maxRemoteVersion' = 0,
    publishedVersion' = writers.mapBy(_ => 0),
  }

  /// A writer prepares a new local manifest whose version is strictly greater
  /// than their previous local version and, if they have already published,
  /// greater than their last published remote version.
  action prepareLocal = {
    nondet w = oneOf(writers)
    nondet v = oneOf(versions.filter(v =>
      v > localVersion.get(w) and v > publishedVersion.get(w)
    ))
    all {
      localVersion' = localVersion.set(w, v),
      remoteVersion' = remoteVersion,
      maxRemoteVersion' = maxRemoteVersion,
      publishedVersion' = publishedVersion,
    }
  }

  /// A writer publishes their local manifest using the correct CAS check:
  /// the upload succeeds only if the remote store is empty (version 0) or holds
  /// a version strictly less than the local version.
  action putManifestCas = {
    nondet w = oneOf(writers)
    val lv = localVersion.get(w)
    val ok = remoteVersion == 0 or remoteVersion < lv
    val nextRemote = if (ok) lv else remoteVersion
    all {
      localVersion' = localVersion,
      remoteVersion' = nextRemote,
      maxRemoteVersion' = if (nextRemote > maxRemoteVersion) nextRemote else maxRemoteVersion,
      publishedVersion' = if (ok) publishedVersion.set(w, lv) else publishedVersion,
    }
  }

  /// Environment action: another writer (or the same writer via a different
  /// path) successfully publishes a manifest, advancing the remote version.
  action concurrentPublish = {
    nondet v = oneOf(versions.filter(v => v > remoteVersion))
    all {
      remoteVersion' = v,
      maxRemoteVersion' = if (v > maxRemoteVersion) v else maxRemoteVersion,
      localVersion' = localVersion,
      publishedVersion' = publishedVersion,
    }
  }

  action step = any {
    prepareLocal,
    putManifestCas,
    concurrentPublish,
  }

  /// The remote version is monotonic: once published, it never reverts to an
  /// older or equal version.
  def remoteVersionMonotonic: bool =
    remoteVersion > 0 implies
      writers.forall(w => publishedVersion.get(w) <= remoteVersion)

  /// A writer's local knowledge of what they published matches reality: if they
  /// think they published version v, the remote version is at least v.
  def publishedMatchesRemote: bool =
    writers.forall(w =>
      publishedVersion.get(w) == 0 or remoteVersion >= publishedVersion.get(w)
    )

  /// No two writers can both believe they published the same positive version
  /// unless the remote store has advanced to at least that version.
  def noStalePublishClaim: bool =
    writers.forall(a =>
      writers.forall(b =>
        a == b
          or publishedVersion.get(a) == 0
          or publishedVersion.get(b) == 0
          or publishedVersion.get(a) != publishedVersion.get(b)
          or remoteVersion >= publishedVersion.get(a)
      )
    )

  /// Remote version never regresses from the maximum ever observed. This is
  /// the property that the CAS check is defending.
  def remoteVersionNotRegressed: bool =
    remoteVersion >= maxRemoteVersion

  def safety: bool = and {
    remoteVersionMonotonic,
    publishedMatchesRemote,
    noStalePublishClaim,
    remoteVersionNotRegressed,
  }

  /// Bad old behavior: the first upload (local version 1) skips the remote
  /// version check and overwrites an existing remote manifest. The action
  /// nondeterministically picks a previously-observed higher remote version
  /// so the single-step negative check demonstrates the regression.
  action badSkipCasOnFirstWrite = {
    nondet w = oneOf(writers)
    nondet prior = oneOf(versions.filter(v => v > 1))
    all {
      localVersion.get(w) == 1,
      localVersion' = localVersion,
      remoteVersion' = 1,
      maxRemoteVersion' = prior,
      publishedVersion' = publishedVersion.set(w, 1),
    }
  }

  /// Bad behavior: publish without reading the remote manifest at all, allowing
  /// an older local manifest to clobber a newer remote one. The action
  /// nondeterministically picks a previously-observed higher remote version
  /// so the single-step negative check demonstrates the regression.
  action badBlindOverwrite = {
    nondet w = oneOf(writers)
    nondet prior = oneOf(versions.filter(v => v > localVersion.get(w)))
    all {
      localVersion' = localVersion,
      remoteVersion' = localVersion.get(w),
      maxRemoteVersion' = prior,
      publishedVersion' = publishedVersion.set(w, localVersion.get(w)),
    }
  }
}
