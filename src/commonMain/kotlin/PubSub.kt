package site.kodev.pubsub

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

@Suppress("UNCHECKED_CAST")
class PubSub {
    private val topics = mutableMapOf<String, Topic<*>>()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val mapMutex = Mutex()
    suspend fun <T : Any> publisher(topicName: String, capacity: Int = Channel.BUFFERED): SendChannel<T>{
        val topic: Topic<T> = mapMutex.withLock {
            (topics[topicName] as? Topic<T>) ?: let {
                val channel = Channel<T>(capacity)
                val mutex = Mutex()
                val subscribers = mutableSetOf<Channel<T>>()
                val dispatcher = scope.launch {
                    for(i in channel){
                        for(j in subscribers){
                            j.send(i)
                        }
                    }
                }
                val topic = Topic(
                    channel,
                    subscribers,
                    mutex,
                    dispatcher
                )
                topics[topicName] = topic
                topic

            }
        }
        return topic.publishChannel
    }
    suspend fun <T : Any> subscribe(topicName: String, capacity: Int = Channel.BUFFERED): ReceiveChannel<T>{
         val topic: Topic<T> = mapMutex.withLock {
             (topics[topicName] as? Topic<T>) ?: let{
                 val channel = Channel<T>(capacity)
                 val mutex = Mutex()
                 val subscribers = mutableSetOf<Channel<T>>()
                 val dispatcher = scope.launch {
                     for(i in channel){
                         for(j in subscribers){
                             j.send(i)
                         }
                     }
                 }
                 val topic = Topic(
                     channel,
                     subscribers,
                     mutex,
                     dispatcher
                 )
                 topics[topicName] = topic
                 topic
             }
         }
        val channel = topic.mutex.withLock {
            val channel = Channel<T>(capacity)
            topic.subscriber.add(channel)
            channel
        }
        return channel
    }
    suspend fun close(){
        mapMutex.withLock {
            topics.forEach { (string, topic) ->
                topic.publishChannel.close()
                topic.dispatcherJob.join()
                topic.subscriber.forEach {
                    it.close()
                }
            }
            topics.clear()
        }
        scope.cancel()
    }
    private data class Topic<T: Any>(
        val publishChannel: Channel<T>,
        val subscriber: MutableSet<Channel<T>>,
        val mutex: Mutex,
        val dispatcherJob: Job
    )
}

