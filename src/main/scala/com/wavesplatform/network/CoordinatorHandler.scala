package com.wavesplatform.network

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesplatform.features.FeatureProvider
import com.wavesplatform.metrics.{BlockStats, HistogramExt}
import com.wavesplatform.mining.Miner
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state2.{ByteStr, trim}
import com.wavesplatform.state2.reader.StateReader
import com.wavesplatform.{Coordinator, UtxPool}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.group.ChannelGroup
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import kamon.Kamon
import monix.eval.Task
import scorex.block.{Block, MicroBlock}
import scorex.transaction.ValidationError.InvalidSignature
import scorex.transaction._
import scorex.utils.{ScorexLogging, Time}

import scala.collection.mutable.{Set => MSet}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@Sharable
class CoordinatorHandler(checkpointService: CheckpointService,
                         history: NgHistory,
                         blockchainUpdater: BlockchainUpdater,
                         time: Time,
                         stateReader: StateReader,
                         utxStorage: UtxPool,
                         blockchainReadiness: AtomicBoolean,
                         miner: Miner,
                         settings: WavesSettings,
                         peerDatabase: PeerDatabase,
                         allChannels: ChannelGroup,
                         featureProvider: FeatureProvider)
  extends ChannelInboundHandlerAdapter with ScorexLogging {

  private val counter = new AtomicInteger
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor { r =>
    val t = new Thread(r)
    t.setName(s"coordinator-handler-${counter.incrementAndGet()}")
    t.setDaemon(true)
    t
  })

  private val processCheckpoint = Coordinator.processCheckpoint(checkpointService, history, blockchainUpdater) _
  private val processFork = Coordinator.processFork(checkpointService, history, blockchainUpdater, stateReader, utxStorage, time, settings, blockchainReadiness, featureProvider) _
  private val processBlock = Coordinator.processSingleBlock(checkpointService, history, blockchainUpdater, time, stateReader, utxStorage, blockchainReadiness, settings, featureProvider) _
  private val processMicroBlock = Coordinator.processMicroBlock(checkpointService, history, blockchainUpdater, utxStorage) _


  private def scheduleMiningAndBroadcastScore(score: BigInt): Unit = {
    miner.scheduleMining()
    allChannels.broadcast(LocalScoreChanged(score))
  }

  private def processAndBlacklistOnFailure[A](src: Channel, start: => String, success: => String, errorPrefix: String,
                                              f: => Either[_, Option[A]],
                                              r: A => Unit): Unit = {
    log.debug(start)
    Future(f) onComplete {
      case Success(Right(maybeNewScore)) =>
        log.debug(success)
        maybeNewScore.foreach(r)
      case Success(Left(ve)) =>
        log.warn(s"$errorPrefix: $ve")
        peerDatabase.blacklistAndClose(src, s"$errorPrefix: $ve")
      case Failure(t) => throw new Exception(errorPrefix, t)
    }
  }

  private def processMicroBlockData(ctx: ChannelHandlerContext, invOpt: Option[MicroBlockInv], microBlock: MicroBlock): Unit = {
    val microblockTotalResBlockSig = microBlock.totalResBlockSig
    Future(Signed.validateSignatures(microBlock).flatMap(processMicroBlock)) onComplete {
      case Success(Right(())) =>
        invOpt match {
          case Some(mi) => allChannels.broadcast(mi, Some(ctx.channel()))
          case None => log.warn("Not broadcasting MicroBlockInv")
        }
        BlockStats.applied(microBlock)
      case Success(Left(is: InvalidSignature)) =>
        peerDatabase.blacklistAndClose(ctx.channel(), s"Could not append microblock $microblockTotalResBlockSig: $is")
      case Success(Left(ve)) =>
        BlockStats.declined(microBlock)
        log.debug(s"Could not append microblock $microblockTotalResBlockSig: $ve")
      case Failure(t) => rethrow(s"Error appending microblock $microblockTotalResBlockSig", t)
    }
  }

  private def rethrow(msg: String, failure: Throwable) = throw new Exception(msg, failure.getCause)

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case c: Checkpoint => processAndBlacklistOnFailure(ctx.channel,
      "Attempting to process checkpoint",
      "Successfully processed checkpoint",
      s"Error processing checkpoint",
      processCheckpoint(c).map(Some(_)), scheduleMiningAndBroadcastScore)

    case ExtensionBlocks(blocks) =>
      blocks.foreach(BlockStats.received(_, BlockStats.Source.Ext, ctx))
      processAndBlacklistOnFailure(ctx.channel,
        s"Attempting to append extension ${formatBlocks(blocks)}",
        s"Successfully appended extension ${formatBlocks(blocks)}",
        s"Error appending extension ${formatBlocks(blocks)}",
        processFork(blocks),
        scheduleMiningAndBroadcastScore)

    case b: Block => Future({
      BlockStats.received(b, BlockStats.Source.Broadcast, ctx)
      blockReceivingLag.safeRecord(System.currentTimeMillis() - b.timestamp)
      Signed.validateSignatures(b).flatMap(b => processBlock(b, false))
    }) onComplete {
      case Success(Right(None)) =>
        log.trace(s"Block ${b.uniqueId} already appended")
      case Success(Right(Some(newScore))) =>
        log.debug(s"Appended block ${b.uniqueId}")
        if (b.transactionData.isEmpty)
          allChannels.broadcast(BlockForged(b), Some(ctx.channel()))
        miner.scheduleMining()
        allChannels.broadcast(LocalScoreChanged(newScore))
      case Success(Left(is: InvalidSignature)) =>
        peerDatabase.blacklistAndClose(ctx.channel(), s"Could not append block ${b.uniqueId}: $is")
      case Success(Left(ve)) =>
        BlockStats.declined(b, BlockStats.Source.Broadcast)
        log.debug(s"Could not append block ${b.uniqueId}: $ve")
      // no need to push anything downstream in here, because channels are pinned only when handling extensions
      case Failure(t) => rethrow(s"Error appending block ${b.uniqueId}", t)
    }

    // From MicroBlockSynchronizer

    case mbr@MicroBlockResponse(mb) =>
      downloading.set(false)
      Task {
        log.trace(id(ctx) + "Received " + mbr)
        knownMicroBlockOwners.invalidate(mb.totalResBlockSig)
        successfullyReceivedMicroBlocks.put(mb.totalResBlockSig, dummy)

        Option(microBlockReceiveTime.getIfPresent(mb.totalResBlockSig)) match {
          case Some(created) =>
            BlockStats.received(mb, ctx, propagationTime = System.currentTimeMillis() - created)
            microBlockReceiveTime.invalidate(mb.totalResBlockSig)
            processMicroBlockData(ctx, Option(awaitingMicroBlocks.getIfPresent(mb.totalResBlockSig)), mb)
          case None =>
            BlockStats.received(mb, ctx)
        }
      }.runAsync(scheduler)
    case mi@MicroBlockInv(_, totalResBlockSig, prevResBlockSig, _) => Task.unit.flatMap { _ =>
      Signed.validateSignatures(mi) match {
        case Left(err) => Task.now(peerDatabase.blacklistAndClose(ctx.channel(), err.toString))
        case Right(_) =>
          log.trace(id(ctx) + "Received " + mi)
          history.lastBlockId() match {
            case Some(lastBlockId) =>
              knownNextMicroBlocks.get(mi.prevBlockSig, { () =>
                BlockStats.inv(mi, ctx)
                mi
              })
              knownMicroBlockOwners.get(totalResBlockSig, () => MSet.empty) += ctx
              microBlockReceiveTime.get(totalResBlockSig, () => System.currentTimeMillis())

              if (lastBlockId == prevResBlockSig) {
                microBlockInvStats.increment()

                if (alreadyRequested(totalResBlockSig)) Task.unit
                else tryDownloadNext(mi.prevBlockSig)
              } else {
                notLastMicroblockStats.increment()
                log.trace(s"Discarding $mi because it doesn't match last (micro)block ${trim(lastBlockId)}")
                Task.unit
              }

            case None =>
              unknownMicroblockStats.increment()
              Task.unit
          }
      }
    }.runAsync(scheduler)
  }

  // From MicroBlockSynchronizer

  import settings.synchronizationSettings.{microBlockSynchronizer => mbsSettings}

  private val scheduler = monix.execution.Scheduler.singleThread("microblock-synchronizer", reporter = com.wavesplatform.utils.UncaughtExceptionsToLogReporter)

  private val awaitingMicroBlocks = cache[MicroBlockSignature, MicroBlockInv](mbsSettings.invCacheTimeout)
  private val knownMicroBlockOwners = cache[MicroBlockSignature, MSet[ChannelHandlerContext]](mbsSettings.invCacheTimeout)
  private val knownNextMicroBlocks = cache[MicroBlockSignature, MicroBlockInv](mbsSettings.invCacheTimeout)
  private val successfullyReceivedMicroBlocks = cache[MicroBlockSignature, Object](mbsSettings.processedMicroBlocksCacheTimeout)
  private val microBlockReceiveTime = cache[MicroBlockSignature, java.lang.Long](mbsSettings.invCacheTimeout)
  private val downloading = new AtomicBoolean(false)
  blockchainUpdater.lastBlockId.foreach { lastBlockSig =>
    tryDownloadNext(lastBlockSig).runAsync(scheduler)
  }(scheduler)

  private def alreadyRequested(microBlockSig: MicroBlockSignature): Boolean = Option(awaitingMicroBlocks.getIfPresent(microBlockSig)).isDefined

  private def alreadyProcessed(microBlockSig: MicroBlockSignature): Boolean = Option(successfullyReceivedMicroBlocks.getIfPresent(microBlockSig)).isDefined

  private def requestMicroBlockTask(microblockInv: MicroBlockInv, attemptsAllowed: Int): Task[Unit] = Task.unit.flatMap { _ =>
    val totalResBlockSig = microblockInv.totalBlockSig
    if (attemptsAllowed > 0 && !alreadyProcessed(totalResBlockSig)) {
      val knownChannels = knownMicroBlockOwners.get(totalResBlockSig, () => MSet.empty)
      random(knownChannels) match {
        case Some(ctx) =>
          knownChannels -= ctx
          ctx.writeAndFlush(MicroBlockRequest(totalResBlockSig))
          awaitingMicroBlocks.put(totalResBlockSig, microblockInv)
          requestMicroBlockTask(microblockInv, attemptsAllowed - 1)
            .delayExecution(mbsSettings.waitResponseTimeout)
        case None => Task.unit
      }
    } else Task.unit
  }

  private def tryDownloadNext(prevBlockId: ByteStr): Task[Unit] = Task.unit.flatMap { _ =>
    Option(knownNextMicroBlocks.getIfPresent(prevBlockId)) match {
      case Some(mb) =>
        if (downloading.compareAndSet(false, true)) requestMicroBlockTask(mb, MicroBlockDownloadAttempts)
        else Task.unit
      case None =>
        Task.unit
    }
  }

  // From MicroBlockSynchronizer companion

  val blockReceivingLag = Kamon.metrics.histogram("block-receiving-lag")

  // From MicroBlockSynchronizer

  type MicroBlockSignature = ByteStr

  val MicroBlockDownloadAttempts = 2

  val microBlockInvStats = Kamon.metrics.registerCounter("micro-inv")

  val notLastMicroblockStats = Kamon.metrics.registerCounter("micro-not-last")
  val unknownMicroblockStats = Kamon.metrics.registerCounter("micro-unknown")

  def random[T](s: MSet[T]): Option[T] = {
    val n = util.Random.nextInt(s.size)
    val ts = s.iterator.drop(n)
    if (ts.hasNext) Some(ts.next)
    else None
  }

  def cache[K <: AnyRef, V <: AnyRef](timeout: FiniteDuration): Cache[K, V] = CacheBuilder.newBuilder()
    .expireAfterWrite(timeout.toMillis, TimeUnit.MILLISECONDS)
    .build[K, V]()

  val dummy = new Object()
}
