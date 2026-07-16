import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import site.kodev.pubsub.PubSub
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class PubSubTest {
    @Test
    fun `basic test`() = runTest {
        val pubsub = PubSub()
        val publisher1 = pubsub.publisher<Int>("topic")
        val publisher2 = pubsub.publisher<Int>("topic")
        val receiver1 = pubsub.subscribe<Int>("topic")
        val receiver2 = pubsub.subscribe<Int>("topic")
        var count1 = 0
        var count2 = 0
        val s1 = launch {
            repeat(500){
                publisher1.send(1)
            }
        }
        val s2 = launch {
            repeat(500){
                publisher2.send(2)
            }
        }
        val j1 = launch {
            for (i in receiver1) {
                count1++
            }
        }
        val j2 = launch{
            for(i in receiver2){
                count2++
            }
        }
        s1.join()
        s2.join()
        pubsub.close()
        j1.join()
        j2.join()
        assertEquals(1000,count1)
        assertEquals(1000,count2)
    }
    @Test
    fun `channel works after pubsub closed`() = runTest {
        val pubsub = PubSub()
        val publisher = pubsub.publisher<Int>("topic")
        val subscriber = pubsub.subscribe<Int>("topic")
        repeat(64){
            publisher.send(1)
        }
        pubsub.close()
        var count = 0
        for(i in subscriber){
            count++
        }
        assertEquals(64, count)
    }
    @Test
    @OptIn(ExperimentalAtomicApi::class)
    fun `multiple topics at same time`() = runTest {
        val pubsub = PubSub()
        val pub1 = pubsub.publisher<Int>("topic1",1000)
        val pub2 = pubsub.publisher<Int>("topic1")
        val pub3 = pubsub.publisher<String>("topic2",1000)
        val pub4 = pubsub.publisher<String>("topic2")
        val rec1 = pubsub.subscribe<Int>("topic1",1000)
        val rec2 = pubsub.subscribe<Int>("topic1",1000)
        val rec3 = pubsub.subscribe<String>("topic2",1000)
        val rec4 = pubsub.subscribe<String>("topic2",1000)

        val s1 = launch {
            repeat(500){
                pub1.send(1)
            }
        }
        val s2 = launch {
            repeat(500){
                pub2.send(2)
            }
        }
        val s3 = launch {
            repeat(500){
                pub3.send("")
            }
        }
        val s4 = launch {
            repeat(500){
                pub4.send("")
            }
        }
        val count = AtomicInt(0)
        val j1 = launch {
            for(i in rec1){
                count.addAndFetch(1)
            }
        }
        val j2 = launch {
            for(i in rec2){
                count.addAndFetch(1)
            }
        }
        val j3 = launch {
            for(i in rec3){
                count.addAndFetch(1)
            }
        }
        val j4 = launch {
            for(i in rec4){
                count.addAndFetch(1)
            }
        }
        s1.join()
        s2.join()
        s3.join()
        s4.join()
        pubsub.close()
        j1.join()
        j2.join()
        j3.join()
        j4.join()
        assertEquals(4000,count.load())
    }
    @Test
    fun `locks all publishers`() = runTest {
        val pubsub = PubSub().lockAll()
        val publisher = pubsub.publisher<Int>("topic1")
        val completable = CompletableDeferred<Int>()
        launch {
            publisher.send(1)
            completable.complete(1)
        }
        delay(1.seconds)
        val sub = pubsub.subscribe<Int>("topic1")
        completable.complete(2)
        pubsub.unlockAll()
        val result = completable.await()
        assertEquals(2, result)
    }
    @Test
    fun `receives values after unlocking topic`() = runTest {
        val pubsub = PubSub().lockAll()
        val publisher = pubsub.publisher<Int>("topic1")
        val completable = CompletableDeferred<Int>()
        launch {
            publisher.send(1)
            completable.complete(1)
        }
        delay(1.seconds)
        val sub = pubsub.subscribe<Int>("topic1")
        completable.complete(2)
        pubsub.unlockAll()
        val result = completable.await()
        val received = sub.receive()
        assertEquals(2, result)
        assertEquals(1, received)
    }
    @Test
    fun `close topic`() = runTest {
        val pubsub = PubSub()
        val publisher = pubsub.publisher<Int>("topic1")
        val rec = pubsub.subscribe<Int>("topic1",32)
        repeat(32){
            publisher.send(1)
        }
        publisher.close()
        var count = 0
        for(i in rec){
            count++
        }
        assertEquals(32, count)
    }
}