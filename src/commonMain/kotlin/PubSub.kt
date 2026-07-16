package site.kodev.pubsub

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelIterator
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectClause1
import kotlinx.coroutines.selects.SelectClause2
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@Suppress("UNCHECKED_CAST")
class PubSub {
    private var isClosed = false
    private var lockNewPublishers = false
    private val topics = mutableMapOf<String, Topic<*>>()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val mapMutex = Mutex()

    /**
     * Lock the topic. When locked, all publishers trying to send to the channel will be suspended until unlocked
     */
    suspend fun lock(topic: String){
        mapMutex.withLock {
            if(isClosed)throw CancellationException("PubSub closed")
            topics[topic]?.publishChannel?.lock() ?: throw Exception("Topic $topic not found")
        }
    }

    /**
     * Unlocks the topic. Publishers can freely send to the channel and if someone was trying to send before unlocking, it unsuspends it and sends to channel
     */
    suspend fun unlock(topic: String){
        mapMutex.withLock {
            if(isClosed)throw CancellationException("PubSub closed")
            topics[topic]?.publishChannel?.unlock() ?: throw Exception("Topic $topic not found")
        }
    }

    /**
     * this locks all existing topics meaning no publisher can send to channels.
     * Any publishers created after calling this function will be locked too.
     * Use this while starting up the application if you want to ensure all receivers are subscribed before publishers can send anything
     */
    suspend fun lockAll(): PubSub{
        mapMutex.withLock {
            if(isClosed)throw CancellationException("PubSub closed")
            for (entry in topics) {
                entry.value.publishChannel.lock()
            }
            lockNewPublishers = true
        }
        return this
    }

    /**
     * unlocks all topics and publishers can now send to it, and new publishers will be unlocked by default
     */
    suspend fun unlockAll(): PubSub{
        mapMutex.withLock {
            if(isClosed)throw CancellationException("PubSub closed")
            for (entry in topics) {
                entry.value.publishChannel.unlock()
            }
            lockNewPublishers = false
        }
        return this
    }
    private fun <T: Any> topic(name: String, capacity: Int = Channel.BUFFERED, locked: Boolean = false): Topic<T>{
        val channel = LockableChannel<T>(capacity)
        // Remove the topic if publisherChannel is closed
        if(lockNewPublishers||locked)channel.lock()
        val mutex = Mutex()
        val subscribers = mutableSetOf<Channel<T>>()
        val dispatcher = scope.launch {
            for(i in channel){
                for(j in subscribers){
                    j.send(i)
                }
            }
            mutex.withLock {
                subscribers.forEach {
                    it.close()
                }
            }
            topics.remove(name)
        }
        val topic = Topic(
            channel,
            subscribers,
            mutex,
            dispatcher
        )
        topics[name] = topic
        return topic
    }

    /**
     * Creates a publisher for a topic. If topic doesn't exist, then creates a new topic
     * @param topicName Name of the topic
     * @param capacity Buffer Capacity of the channel (default: Channel.Bufferred)
     * @param locked Creates a locked topic meaning publishers can't send data to the channel, before unlocked
     * Call SendChannel.close() function to close the topic.
     */
    suspend fun <T : Any> publisher(topicName: String, capacity: Int = Channel.BUFFERED, locked: Boolean=false): SendChannel<T>{
        val topic: Topic<T> = mapMutex.withLock {
            if(isClosed)throw CancellationException("PubSub closed")
            (topics[topicName] as? Topic<T>) ?: topic(topicName, capacity)
        }
        return topic.publishChannel
    }

    /**
     * Creates a subscriber for a topic. Creates new topic if it doesn't exist
     */
    suspend fun <T : Any> subscribe(topicName: String, capacity: Int = Channel.BUFFERED): ReceiveChannel<T>{
         val topic: Topic<T> = mapMutex.withLock {
             if(isClosed)throw CancellationException("PubSub closed")
             (topics[topicName] as? Topic<T>) ?: topic(topicName, capacity)
         }
        val channel = topic.mutex.withLock {
            val channel = Channel<T>(capacity)
            topic.subscriber.add(channel)
            channel
        }
        return channel
    }

    /**
     * closes all publishers and subscribers channels. Subscribers can still read buffered data
     */
    suspend fun close(){
        val snapshot = mapMutex.withLock {
            isClosed = true
            topics.values.toList()
        }

        snapshot.forEach { topic ->
            topic.publishChannel.close()
            topic.dispatcherJob.join()
        }


        scope.cancel()
    }
    private data class Topic<T: Any>(
        val publishChannel: LockableChannel<T>,
        val subscriber: MutableSet<Channel<T>>,
        val mutex: Mutex,
        val dispatcherJob: Job
    )
    class LockableChannel<T: Any>(capacity: Int): Channel<T>{
        @Volatile
        private var gate = CompletableDeferred<Unit>().apply { complete(Unit) }
        private val channel = Channel<T>(capacity)
        @DelicateCoroutinesApi
        override val isClosedForSend: Boolean
            get() = channel.isClosedForSend
        override val onSend: SelectClause2<T, SendChannel<T>>
            get() = channel.onSend

        override suspend fun send(element: T) {
            gate.await()
            channel.send(element)
        }
        fun lock(){
            this.gate = CompletableDeferred()
        }
        fun unlock(){
            this.gate.complete(Unit)
        }
        @OptIn(InternalCoroutinesApi::class)
        override fun trySend(element: T): ChannelResult<Unit> {
            return if(gate.isCompleted){
                channel.trySend(element)
            }else{
                ChannelResult.failure()
            }
        }

        override fun close(cause: Throwable?): Boolean {
            return channel.close()
        }

        override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
            channel.invokeOnClose(handler)
        }

        @DelicateCoroutinesApi
        override val isClosedForReceive: Boolean
            get() = channel.isClosedForReceive

        @ExperimentalCoroutinesApi
        override val isEmpty: Boolean
            get() = channel.isEmpty
        override val onReceive: SelectClause1<T>
            get() = channel.onReceive
        override val onReceiveCatching: SelectClause1<ChannelResult<T>>
            get() = channel.onReceiveCatching

        override suspend fun receive(): T {
            return channel.receive()
        }

        override suspend fun receiveCatching(): ChannelResult<T> {
            return channel.receiveCatching()
        }

        override fun tryReceive(): ChannelResult<T> {
            return channel.tryReceive()
        }

        override fun iterator(): ChannelIterator<T> {
            return channel.iterator()
        }

        override fun cancel(cause: CancellationException?) {
            channel.cancel()
        }

        @Deprecated("Since 1.2.0, binary compatibility with versions <= 1.1.x", level = DeprecationLevel.HIDDEN)
        override fun cancel(cause: Throwable?): Boolean {
            TODO("Not yet implemented")
        }

    }
}

