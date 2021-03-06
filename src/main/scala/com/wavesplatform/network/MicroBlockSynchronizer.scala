package com.wavesplatform.network

import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesplatform.metrics.BlockStats
import com.wavesplatform.network.MicroBlockSynchronizer._
import com.wavesplatform.settings.SynchronizationSettings.MicroblockSynchronizerSettings
import com.wavesplatform.state2.ByteStr
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import monix.eval.Task
import monix.execution.CancelableFuture
import monix.execution.schedulers.SchedulerService
import scorex.block.MicroBlock
import monix.reactive.Observable
import scorex.transaction.{NgHistory, Signed}
import scorex.utils.ScorexLogging

import scala.collection.mutable.{Set => MSet}
import scala.concurrent.duration.FiniteDuration

@Sharable
class MicroBlockSynchronizer(settings: MicroblockSynchronizerSettings,
                             history: NgHistory,
                             peerDatabase: PeerDatabase,
                             lastBlockIdEvents: Observable[ByteStr]) extends ChannelInboundHandlerAdapter with ScorexLogging {

  private implicit val scheduler: SchedulerService = monix.execution.Scheduler.singleThread(
    "microblock-synchronizer",
    reporter = com.wavesplatform.utils.UncaughtExceptionsToLogReporter
  )

  private val knownOwners = cache[MicroBlockSignature, MSet[ChannelHandlerContext]](settings.invCacheTimeout)
  private val nextInvs = cache[MicroBlockSignature, MicroBlockInv](settings.invCacheTimeout)
  private val awaiting = cache[MicroBlockSignature, MicroBlockInv](settings.invCacheTimeout)
  private val successfullyReceived = cache[MicroBlockSignature, Object](settings.processedMicroBlocksCacheTimeout)

  lastBlockIdEvents.foreach(tryDownloadNext)

  private def alreadyRequested(totalSig: MicroBlockSignature): Boolean = Option(awaiting.getIfPresent(totalSig)).isDefined

  private def alreadyProcessed(totalSig: MicroBlockSignature): Boolean = Option(successfullyReceived.getIfPresent(totalSig)).isDefined

  private def requestMicroBlock(mbInv: MicroBlockInv): CancelableFuture[Unit] = {
    import mbInv.totalBlockSig

    def pollOwner: Option[ChannelHandlerContext] = {
      val owners = knownOwners.get(totalBlockSig, () => MSet.empty)
      random(owners).map { ctx =>
        owners -= ctx
        ctx
      }
    }

    def task(attemptsAllowed: Int): Task[Unit] = Task.unit.flatMap { _ =>
      if (attemptsAllowed <= 0 || alreadyProcessed(totalBlockSig)) Task.unit
      else pollOwner.fold(Task.unit) { ownerCtx =>
        if (ownerCtx.channel().isOpen) {
          ownerCtx.writeAndFlush(MicroBlockRequest(totalBlockSig))
          awaiting.put(totalBlockSig, mbInv)
          task(attemptsAllowed - 1).delayExecution(settings.waitResponseTimeout)
        } else task(attemptsAllowed)
      }
    }

    task(MicroBlockDownloadAttempts).runAsync
  }

  private def tryDownloadNext(prevBlockId: ByteStr): Unit = Option(nextInvs.getIfPresent(prevBlockId)).foreach(requestMicroBlock)

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case MicroBlockResponse(mb) =>
      import mb.{totalResBlockSig => totalSig}

      successfullyReceived.put(totalSig, dummy)
      knownOwners.invalidate(totalSig)
      BlockStats.received(mb, ctx)

      Task {
        log.trace(s"${id(ctx)} Received $msg")
        Option(awaiting.getIfPresent(totalSig)).foreach { mi =>
          awaiting.invalidate(totalSig)
          super.channelRead(ctx, MicroblockData(Option(mi), mb))
        }
      }.runAsync

    case mbInv@MicroBlockInv(_, totalSig, prevSig, _) => Task {
      Signed.validateSignatures(mbInv) match {
        case Left(err) => peerDatabase.blacklistAndClose(ctx.channel(), err.toString)
        case Right(_) =>
          log.trace(s"${id(ctx)} Received $msg")
          knownOwners.get(totalSig, () => MSet.empty) += ctx
          nextInvs.get(prevSig, { () =>
            BlockStats.inv(mbInv, ctx)
            mbInv
          })

          history.lastBlockId()
            .filter(_ == prevSig && !alreadyRequested(totalSig))
            .foreach(tryDownloadNext)
      }
    }.runAsync

    case _ => super.channelRead(ctx, msg)
  }
}

object MicroBlockSynchronizer {

  case class MicroblockData(invOpt: Option[MicroBlockInv], microBlock: MicroBlock)

  type MicroBlockSignature = ByteStr

  private val MicroBlockDownloadAttempts = 2

  def random[T](s: MSet[T]): Option[T] = {
    val n = util.Random.nextInt(s.size)
    val ts = s.iterator.drop(n)
    if (ts.hasNext) Some(ts.next)
    else None
  }

  def cache[K <: AnyRef, V <: AnyRef](timeout: FiniteDuration): Cache[K, V] = CacheBuilder.newBuilder()
    .expireAfterWrite(timeout.toMillis, TimeUnit.MILLISECONDS)
    .build[K, V]()

  private val dummy = new Object()
}
