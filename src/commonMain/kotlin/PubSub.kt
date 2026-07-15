package site.kodev.kotlin_pubsub

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass


class PubSub(
    coroutineContext: CoroutineContext?=null,
    val defaultChannelCapacity: Int = 100
)  {
    private val map = HashMap<String, DataStream<*>>()
    private val mapMutex = Mutex()
    private val parentListener = coroutineContext?.let { CoroutineScope(it + SupervisorJob()) } ?: CoroutineScope(Dispatchers.Default + SupervisorJob())

    /**
     * Create a publisher with name and channelCapacity.
     */
    suspend fun <T: Any> publisher(name: String, channelCapacity: Int = defaultChannelCapacity): SendChannel<T>{
        val publisherChannel =  Channel<T>(capacity = channelCapacity)
        val dataStream = DataStream(
            publisherChannel,
            name,
            Mutex(),
            mutableSetOf(),
        )
        mapMutex.withLock {
             map.putIfAbsent(name, dataStream) ?: throw Exception("Channel Already exists for name $name")
        }
        // Listener for channel
        parentListener.launch {
            for(i in publisherChannel){
                parentListener.launch {
                    dataStream.subscribers.forEach {
                        it.send(i)
                    }
                }
            }
        }
        return dataStream.publisherChannel
    }

    /**
     * Create a publisher with enum
     */
    suspend fun <T: Any> publisher(enum: Enum<*>, channelCapacity: Int = defaultChannelCapacity): SendChannel<T> {
        val name = enum.name
        return publisher(name, channelCapacity)
    }
    suspend fun <T: Any> publisher(clazz: KClass<*>, channelCapacity: Int = defaultChannelCapacity): SendChannel<T> {
        val name = clazz.simpleName!!
        return publisher(name, channelCapacity)
    }
    suspend fun <T: Any> consumer(enum: Enum<*>, channelCapacity: Int = defaultChannelCapacity): ReceiveChannel<T>{
        val name = enum.name
        return consumer(name, channelCapacity)
    }
    suspend fun <T: Any> consumer(clazz: KClass<*>, channelCapacity: Int = defaultChannelCapacity): ReceiveChannel<T>{
        val name = clazz.simpleName!!
        return consumer(name, channelCapacity)
    }
    suspend fun <T: Any> consumer(name: String, channelCapacity: Int = defaultChannelCapacity): ReceiveChannel<T>{
        val dataStream = mapMutex.withLock {
            map[name] ?: throw Exception("No Channel for name $name")
        }
        @Suppress("UNCHECKED_CAST")
        val castedDataStream = runCatching {
            dataStream as DataStream<T>
        }.getOrElse {
            throw Exception("Couldn't cast DataStream",it)
        }
        val receiveChannel = Channel<T>(100)
        castedDataStream.sharedSubscriberMutex.withLock {
            castedDataStream.subscribers.add(receiveChannel)
        }
        return receiveChannel
    }
    suspend fun close(){
        mapMutex.withLock {
            map.forEach { (_, stream) ->
                stream.publisherChannel.close()
                parentListener.launch {
                    stream.sharedSubscriberMutex.withLock {
                        stream.subscribers.forEach {
                            it.close()
                        }
                    }
                }
                map.clear()
            }
        }
        parentListener.cancel()
    }
    fun cancel(){
        parentListener.cancel()
    }
}

private data class DataStream<T : Any>(
    val publisherChannel: SendChannel<T>,
    val channelName: String,
    val sharedSubscriberMutex: Mutex,
    val subscribers: MutableSet<Channel<T>>
)