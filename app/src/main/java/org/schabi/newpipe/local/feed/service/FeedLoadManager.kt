package org.schabi.newpipe.local.feed.service

import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.preference.PreferenceManager
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Notification
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.functions.Consumer
import io.reactivex.rxjava3.processors.PublishProcessor
import io.reactivex.rxjava3.schedulers.Schedulers
import org.schabi.newpipe.R
import org.schabi.newpipe.database.feed.model.FeedGroupEntity
import org.schabi.newpipe.database.subscription.NotificationMode
import org.schabi.newpipe.extractor.Info
import org.schabi.newpipe.extractor.InfoItem
import org.schabi.newpipe.extractor.NewPipe
import org.schabi.newpipe.extractor.Page
import org.schabi.newpipe.extractor.ServiceList
import org.schabi.newpipe.extractor.feed.FeedInfo
import org.schabi.newpipe.extractor.ListInfo
import org.schabi.newpipe.extractor.exceptions.ContentNotAvailableException
import org.schabi.newpipe.extractor.exceptions.ContentNotSupportedException
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException
import org.schabi.newpipe.extractor.linkhandler.ChannelTabs
import org.schabi.newpipe.extractor.linkhandler.ListLinkHandler
import org.schabi.newpipe.extractor.services.bilibili.BilibiliService
import org.schabi.newpipe.extractor.stream.StreamInfoItem
import org.schabi.newpipe.error.ReCaptchaActivity
import org.schabi.newpipe.local.feed.FeedDatabaseManager
import org.schabi.newpipe.local.subscription.SubscriptionManager
import org.schabi.newpipe.util.ChannelTabHelper
import org.schabi.newpipe.util.ExtractorHelper.getChannelInfo
import org.schabi.newpipe.util.ExtractorHelper.getChannelTab
import org.schabi.newpipe.util.ExtractorHelper.getMoreChannelTabItems
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class FeedLoadManager(private val context: Context) {

    private val subscriptionManager = SubscriptionManager(context)
    private val feedDatabaseManager = FeedDatabaseManager(context)

    private val notificationUpdater = PublishProcessor.create<String>()
    private val currentProgress = AtomicInteger(-1)
    private val maxProgress = AtomicInteger(-1)
    private val cancelSignal = AtomicBoolean()
    private val feedResultsHolder = FeedResultsHolder()

    private val lastFetchTimePerService = ConcurrentHashMap<Int, Long>()

    /**
     * Guards the reservation of the next per-service fetch slot, see [startLoading].
     */
    private val rateLimiterLock = Any()


    val notification: Flowable<FeedLoadState> = notificationUpdater.map { description ->
        FeedLoadState(description, maxProgress.get(), currentProgress.get())
    }


    private fun getServiceDelay(serviceId: Int): Long {
        return NewPipe.getService(serviceId).feedFetchInterval;
    }

    /**
     * Start checking for new streams of a subscription group.
     * @param groupId The ID of the subscription group to load. When using
     * [FeedGroupEntity.GROUP_ALL_ID], all subscriptions are loaded. When using
     * [GROUP_NOTIFICATION_ENABLED], only subscriptions with enabled notifications for new streams
     * are loaded. Using an id of a group created by the user results in that specific group to be
     * loaded.
     * @param ignoreOutdatedThreshold When `false`, only subscriptions which have not been updated
     * within the `feed_update_threshold` are checked for updates. This threshold can be set by
     * the user in the app settings. When `true`, all subscriptions are checked for new streams.
     */
    fun startLoading(
        groupId: Long = FeedGroupEntity.GROUP_ALL_ID,
        ignoreOutdatedThreshold: Boolean = false,
    ): Single<List<Notification<FeedUpdateInfo>>> {
        val defaultSharedPreferences = PreferenceManager.getDefaultSharedPreferences(context)
        val useFeedExtractor = defaultSharedPreferences.getBoolean(
            context.getString(R.string.feed_use_dedicated_fetch_method_key),
            false
        )

        val filterFutureItems = defaultSharedPreferences.getBoolean(
            context.getString(R.string.filter_future_items_key),
            true
        )

        val outdatedThreshold = if (ignoreOutdatedThreshold) {
            OffsetDateTime.now(ZoneOffset.UTC)
        } else {
            val thresholdOutdatedSeconds = (
                    defaultSharedPreferences.getString(
                        context.getString(R.string.feed_update_threshold_key),
                        context.getString(R.string.feed_update_threshold_default_value)
                    ) ?: context.getString(R.string.feed_update_threshold_default_value)
                    ).toInt()
            OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(thresholdOutdatedSeconds.toLong())
        }

        /**
         * subscriptions which have not been updated within the feed updated threshold
         */
        val outdatedSubscriptions = when (groupId) {
            FeedGroupEntity.GROUP_ALL_ID -> feedDatabaseManager.outdatedSubscriptions(
                outdatedThreshold
            )
            GROUP_NOTIFICATION_ENABLED -> feedDatabaseManager.outdatedSubscriptionsWithNotificationMode(
                outdatedThreshold, NotificationMode.ENABLED
            )
            else -> feedDatabaseManager.outdatedSubscriptionsForGroup(groupId, outdatedThreshold)
        }

        return outdatedSubscriptions
            .take(1)
            .doOnNext {
                currentProgress.set(0)
                maxProgress.set(it.size)
            }
            .filter { it.isNotEmpty() }
            .observeOn(AndroidSchedulers.mainThread())
            .doOnNext {
                notificationUpdater.onNext("")
                broadcastProgress()
            }
            .observeOn(Schedulers.io())
            .flatMap { Flowable.fromIterable(it) }
            .takeWhile { !cancelSignal.get() }
            .parallel(PARALLEL_EXTRACTIONS, PARALLEL_EXTRACTIONS * 2)
            .runOn(Schedulers.io(), PARALLEL_EXTRACTIONS * 2)
            .filter { !cancelSignal.get() }
            .concatMap { subscriptionEntity ->
                val serviceId = subscriptionEntity.serviceId
                val delay = getServiceDelay(serviceId)

                // The slot used to be booked in the map() *after* the delay, with a plain
                // read-modify-write in between: every parallel rail could read the same
                // "last fetch" value and start at once, which defeated feedFetchInterval
                // completely. Reserve the slot atomically instead, so parallel rails get
                // 0, delay, 2 * delay, ... and the interval actually holds.
                val wait = synchronized(rateLimiterLock) {
                    val now = System.currentTimeMillis()
                    val lastFetch = lastFetchTimePerService[serviceId] ?: 0L
                    val waitForSlot = maxOf(0L, delay - (now - lastFetch))
                    lastFetchTimePerService[serviceId] = now + waitForSlot
                    waitForSlot
                }

                Flowable.just(subscriptionEntity)
                    .delay(wait, TimeUnit.MILLISECONDS)
                    .flatMap { sub ->
                        Flowable.defer {
                            var error: Throwable? = null
                            val storeOriginalErrorAndRethrow = { e: Throwable ->
                                Log.e(TAG, "Failed to extract from channel '${subscriptionEntity.name}' (${subscriptionEntity.serviceId}:${subscriptionEntity.url}): ${e.javaClass.simpleName}: ${e.message}", e)
                                error = e
                                throw e
                            }

                            try {
                                // check for and load new streams
                                // either by using the dedicated feed method or by getting the channel info
                                var originalInfo: Info? = null
                                var streams: List<StreamInfoItem>? = null
                                val errors = ArrayList<Throwable>()

                                if (useFeedExtractor) {
                                    NewPipe.getService(subscriptionEntity.serviceId)
                                        .getFeedExtractor(subscriptionEntity.url)
                                        ?.also { feedExtractor ->
                                            // the user wants to use a feed extractor and there is one, use it
                                            val feedInfo = FeedInfo.getInfo(feedExtractor)
                                            errors.addAll(feedInfo.errors)
                                            originalInfo = feedInfo
                                            streams = feedInfo.relatedItems
                                        }
                                }

                                if (originalInfo == null) {
                                    // use the normal channel tabs extractor if either the user wants it, or
                                    // the current service does not have a dedicated feed extractor

                                    // Hand BiliBili what this subscription already knows, so a
                                    // feed refresh does not have to download the user card for
                                    // every channel it walks over. The extractor only re-downloads
                                    // it when the uploader renamed themselves or nothing is known.
                                    if (subscriptionEntity.serviceId ==
                                        ServiceList.BiliBili.getServiceId()
                                    ) {
                                        BilibiliService.setKnownChannelCard(
                                            BilibiliService.ChannelCard(
                                                subscriptionEntity.name,
                                                subscriptionEntity.avatarUrl,
                                                subscriptionEntity.subscriberCount,
                                                subscriptionEntity.description
                                            )
                                        )
                                    }
                                    val channelInfo = getChannelInfo(
                                        subscriptionEntity.serviceId,
                                        subscriptionEntity.url, true
                                    )
                                        .onErrorReturn(storeOriginalErrorAndRethrow)
                                        .blockingGet()
                                    errors.addAll(channelInfo.errors)
                                    originalInfo = channelInfo

                                    // A refresh stops as soon as it meets videos it already
                                    // stored. A subscription whose stored history is still
                                    // shallower than the window the feed keeps would never get
                                    // the older ones that way, so while it is still catching up
                                    // the pages are walked to the end of that window instead.
                                    // Once caught up this is false again and costs nothing.
                                    val oldestStored = feedDatabaseManager
                                        .getOldestStoredDate(subscriptionEntity.uid)
                                    val oldestAllowed =
                                        FeedDatabaseManager.FEED_OLDEST_ALLOWED_DATE
                                    val catchingUpOnHistory = oldestStored == null ||
                                        !oldestStored.isBefore(oldestAllowed)

                                    streams = channelInfo.tabs
                                        .filter { tab ->
                                            ChannelTabHelper.fetchFeedChannelTab(
                                                context,
                                                defaultSharedPreferences,
                                                tab
                                            )
                                        }
                                        .flatMap { linkHandler ->
                                            val tabItems: List<InfoItem>
                                            val nextPage: Page?

                                            if (isRedundantVideosTab(subscriptionEntity.serviceId, linkHandler) &&
                                                channelInfo.relatedItems.isNotEmpty()
                                            ) {
                                                // The channel info we just loaded *is* this tab on
                                                // BiliBili, so reuse it instead of letting the tab
                                                // extractor download exactly the same data again.
                                                tabItems = channelInfo.relatedItems
                                                nextPage = channelInfo.nextPage
                                            } else {
                                                val channelTabInfo = getChannelTab(
                                                    subscriptionEntity.serviceId, linkHandler, true
                                                )
                                                    .onErrorReturn(storeOriginalErrorAndRethrow)
                                                    .blockingGet()
                                                errors.addAll(channelTabInfo.errors)
                                                tabItems = channelTabInfo.relatedItems
                                                nextPage = channelTabInfo.nextPage
                                            }

                                            // This used to fetch a second page only when the
                                            // first one came back empty. A channel can publish
                                            // more videos between two refreshes than fit on one
                                            // page though, and everything that scrolled off the
                                            // page was silently missed. Keep walking pages while
                                            // they do not contain anything already stored, so a
                                            // refresh always reaches known ground before stopping.
                                            val collected = ArrayList<InfoItem>(tabItems)
                                            var knownGround = collected.any { it.isStored() }
                                            var page: Page? = nextPage
                                            var extraPages = 0
                                            while (page != null &&
                                                extraPages < MAX_EXTRA_PAGES &&
                                                (catchingUpOnHistory || !knownGround)
                                            ) {
                                                val infoItemsPage = getMoreChannelTabItems(
                                                    subscriptionEntity.serviceId, linkHandler, page
                                                )
                                                    .blockingGet()
                                                errors.addAll(infoItemsPage.errors)
                                                collected += infoItemsPage.items
                                                // Only the newly fetched page has to be looked up;
                                                // earlier pages were checked when they arrived.
                                                knownGround = infoItemsPage.items.any { it.isStored() }
                                                // A channel whose videos have all left the
                                                // retention window can never catch up, and its
                                                // stored date stays empty forever - walking
                                                // further pages would only repeat itself.
                                                if (infoItemsPage.items.isNotEmpty() &&
                                                    infoItemsPage.items.all { it.isBeforeFeed() }
                                                ) {
                                                    page = null
                                                    break
                                                }
                                                page = infoItemsPage.nextPage
                                                extraPages++
                                            }
                                            collected
                                        }
                                        .filterIsInstance<StreamInfoItem>()
                                    // Cleared only now: the pages a catch-up walk fetches must
                                    // not download the user card again. That endpoint is the
                                    // one most likely to answer 412 during a refresh, and one
                                    // refused card request fails the whole channel.
                                    BilibiliService.clearKnownChannelCard()
                                }
                                streams = streams?.filterNot { it.isRoundPlayStream || (filterFutureItems && it.uploadDate != null && it.uploadDate!!.offsetDateTime().isAfter(OffsetDateTime.now())) }

                                return@defer Flowable.just(
                                    FeedUpdateInfo(
                                        subscriptionEntity,
                                        originalInfo!!,
                                        streams!!,
                                        errors,
                                    )
                                )
                            } catch (e: Throwable) {
                                BilibiliService.clearKnownChannelCard()
                                // Actually throw the exception to trigger retry
                                val request = "${sub.serviceId}:${sub.url}"
                                val wrapper = FeedLoadService.RequestException(
                                    sub.uid,
                                    request,
                                    error ?: e
                                )
                                throw wrapper
                            }
                        }
                            .retry { retryCount, throwable ->
                                // Check if it's the actual wrapped exception
                                val actualException = when (throwable) {
                                    is FeedLoadService.RequestException -> throwable.cause
                                    else -> throwable
                                }
                                // Give a channel a second chance before reporting it: most
                                // failures are transient - risk control, a timeout, a dropped
                                // connection - and the retry is what lets a rotated identity or
                                // a fresh connection succeed. Content that the service does not
                                // support will not become supported by asking again.
                                retryCount <= 1 && actualException !is ContentNotSupportedException
                            }
                            // Now wrap in Notification AFTER retry logic
                            .map { feedUpdateInfo ->
                                Notification.createOnNext(feedUpdateInfo)
                            }
                            .onErrorReturn { error ->
                                Notification.createOnError<FeedUpdateInfo>(error)
                            }
                    }

            }

            .sequential()
            .observeOn(AndroidSchedulers.mainThread())
            .doOnNext(NotificationConsumer())
            .observeOn(Schedulers.io())
            .buffer(BUFFER_COUNT_BEFORE_INSERT)
            .doOnNext(DatabaseConsumer())
            .subscribeOn(Schedulers.io())
            .toList()
            .flatMap { x -> postProcessFeed().toSingleDefault(x.flatten()) }
    }

    /**
     * BiliBili's channel extractor returns the videos tab as the channel's initial page, so
     * fetching that tab downloads the very same endpoints a second time: measured on the wire,
     * a subscription costs six requests instead of three (user card, live status and first
     * video page, twice). Reusing the channel info therefore halves the request count of a
     * BiliBili feed refresh - which is exactly what provokes the anti-bot risk control.
     */
    /** True when the feed database already holds this item. */
    private fun InfoItem.isStored(): Boolean =
        this is StreamInfoItem && feedDatabaseManager.doesStreamExist(this)

    /** True when the feed would not keep this item because it is too old. */
    private fun InfoItem.isBeforeFeed(): Boolean {
        val date = (this as? StreamInfoItem)?.uploadDate ?: return false
        return date.offsetDateTime().isBefore(FeedDatabaseManager.FEED_OLDEST_ALLOWED_DATE)
    }

    private fun isRedundantVideosTab(serviceId: Int, tab: ListLinkHandler): Boolean =
        serviceId == ServiceList.BiliBili.getServiceId() &&
            tab.contentFilters.firstOrNull()?.name == ChannelTabs.VIDEOS

    fun cancel() {
        cancelSignal.set(true)
    }

    private fun broadcastProgress() {
        FeedEventManager.postEvent(
            FeedEventManager.Event.ProgressEvent(
                currentProgress.get(),
                maxProgress.get()
            )
        )
    }

    /**
     * Keep the feed and the stream tables small
     * to reduce loading times when trying to display the feed.
     * <br>
     * Remove streams from the feed which are older than [FeedDatabaseManager.FEED_OLDEST_ALLOWED_DATE].
     * Remove streams from the database which are not linked / used by any table.
     */
    private fun postProcessFeed() = Completable.fromRunnable {
        FeedEventManager.postEvent(FeedEventManager.Event.ProgressEvent(R.string.feed_processing_message))
        feedDatabaseManager.removeOrphansOrOlderStreams()

        FeedEventManager.postEvent(FeedEventManager.Event.SuccessResultEvent(feedResultsHolder.itemsErrors))
    }.doOnSubscribe {
        currentProgress.set(-1)
        maxProgress.set(-1)

        notificationUpdater.onNext(context.getString(R.string.feed_processing_message))
        FeedEventManager.postEvent(FeedEventManager.Event.ProgressEvent(R.string.feed_processing_message))
    }.subscribeOn(Schedulers.io())

    private inner class NotificationConsumer : Consumer<Notification<FeedUpdateInfo>> {
        override fun accept(item: Notification<FeedUpdateInfo>) {
            currentProgress.incrementAndGet()
            notificationUpdater.onNext(item.value?.name.orEmpty())

            broadcastProgress()
        }
    }

    /**
     * A challenge a human can clear is not worth reporting as an ordinary failure: the feed runs
     * without a screen, so offer it as a notification instead. Solving it leaves behind the
     * cookies the next attempt needs, and the notification starts the refresh again once done.
     */
    private fun offerHumanVerification(errors: List<Throwable>) {
        val captcha = errors.asSequence().mapNotNull { findCaptcha(it) }.firstOrNull() ?: return

        val intent = Intent(context, ReCaptchaActivity::class.java)
            .putExtra(ReCaptchaActivity.RECAPTCHA_URL_EXTRA, captcha.url)
            .putExtra(ReCaptchaActivity.RECAPTCHA_REFRESH_EXTRA, true)
            .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        val pendingIntent = PendingIntent.getActivity(
            context, 0, intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val notification = NotificationCompat
            .Builder(context, context.getString(R.string.notification_channel_id))
            .setSmallIcon(R.drawable.ic_pipepipe)
            .setContentTitle(context.getString(R.string.recaptcha_request_toast))
            .setContentText(context.getString(R.string.recaptcha_solve))
            .setContentIntent(pendingIntent)
            .setAutoCancel(true)
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .build()

        NotificationManagerCompat.from(context).notify(CAPTCHA_NOTIFICATION_ID, notification)
    }

    /** The challenge hidden somewhere in a failure, if there is one. */
    private fun findCaptcha(error: Throwable): ReCaptchaException? {
        var current: Throwable? = error
        var depth = 0
        while (current != null && depth++ < 8) {
            if (current is ReCaptchaException) {
                return current
            }
            current = current.cause
        }
        return null
    }

    private inner class DatabaseConsumer : Consumer<List<Notification<FeedUpdateInfo>>> {

        override fun accept(list: List<Notification<FeedUpdateInfo>>) {
            feedDatabaseManager.database().runInTransaction {
                for (notification in list) {
                    when {
                        notification.isOnNext -> {
                            val info = notification.value!!
                            val reportableErrors = info.errors.filterNot {
                                it is ContentNotSupportedException
                            }

                            notification.value!!.newStreams = filterNewStreams(info.streams)

                            feedDatabaseManager.upsertAll(info.uid, info.streams)
                            subscriptionManager.updateFromInfo(info)

                            if (reportableErrors.isNotEmpty()) {
                                // Log errors for this specific channel
                                Log.w(TAG, "Channel '${info.name}' (${info.serviceId}:${info.url}) had ${reportableErrors.size} error(s):")
                                reportableErrors.forEachIndexed { index, error ->
                                    Log.w(TAG, "  Error ${index + 1}: ${error.javaClass.simpleName}: ${error.message}", error)
                                }

                                feedResultsHolder.addErrors(
                                    reportableErrors.map {
                                        FeedLoadService.RequestException(
                                            info.uid,
                                            "${info.serviceId}:${info.url}",
                                            it
                                        )
                                    }
                                )
                                feedDatabaseManager.markAsOutdated(info.uid)
                                offerHumanVerification(reportableErrors)
                            }
                        }
                        notification.isOnError -> {
                            val error = notification.error!!

                            // Log the error for the failed channel
                            if (error is FeedLoadService.RequestException) {
                                Log.e(TAG, "Failed to load channel with subscription ID ${error.subscriptionId}: ${error.message}", error.cause ?: error)
                                feedDatabaseManager.markAsOutdated(error.subscriptionId)
                                offerHumanVerification(listOf(error))
                            } else {
                                Log.e(TAG, "Unexpected error during feed loading: ${error.javaClass.simpleName}: ${error.message}", error)
                            }

                            feedResultsHolder.addError(error)
                        }
                    }
                }
            }
        }

        private fun filterNewStreams(list: List<StreamInfoItem>): List<StreamInfoItem> {
            return list.filter {
                !feedDatabaseManager.doesStreamExist(it) &&
                        it.uploadDate != null &&
                        // Streams older than this date are automatically removed from the feed.
                        // Therefore, streams which are not in the database,
                        // but older than this date, are considered old.
                        it.uploadDate!!.offsetDateTime().isAfter(
                            FeedDatabaseManager.FEED_OLDEST_ALLOWED_DATE
                        )
            }
        }
    }

    companion object {

        private val TAG = FeedLoadManager::class.java.simpleName

        /**
         * Constant used to check for updates of subscriptions with [NotificationMode.ENABLED].
         */
        const val GROUP_NOTIFICATION_ENABLED = -2L

        /**
         * How many extractions will be running in parallel.
         */
        private const val PARALLEL_EXTRACTIONS = 6

        /**
         * Number of items to buffer to mass-insert in the database.
         */
        private const val BUFFER_COUNT_BEFORE_INSERT = 20

        /**
         * How many *extra* pages a refresh may walk while it has not met a video it already
         * stored, so five pages in total. Only the first refresh of a subscription, or one after
         * a long gap, pays for this: as soon as a page contains something already stored the
         * walk stops.
         */
        private const val MAX_EXTRA_PAGES = 4

        /** Separate from the progress notification, so it can survive the refresh ending. */
        private const val CAPTCHA_NOTIFICATION_ID = 4561
    }
}
