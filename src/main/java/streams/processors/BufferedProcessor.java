package streams.processors;

import java.time.Duration;
import java.util.ArrayList;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class BufferedProcessor<K, V> implements Processor<K, V, K, V> {

    private final Long bufferTimeMs;
    private final String scheduledTimeStoreName;
    private final String queueStoreName;
    private final Duration flushFrequency;

    private boolean bufferringEnabled;

    private ProcessorContext context;

    private final String bufferingStateStoreName;

    private KeyValueStore<K, V> bufferingStateStore;

    // key -> time in epoch milliseconds
    // value -> Key (eg: activity_id)
    private KeyValueStore<Long, K> queueStore;

    // key -> K
    // value -> time in epoch milliseconds
    private KeyValueStore<K, Long> scheduledTimeStore;

    public BufferedProcessor(Long bufferDuration, Long flushFrequencyDuration, String queueStoreName,
                             String scheduledTimeStoreName, String bufferingStateStoreName, boolean bufferringEnabled) {
        bufferTimeMs = Duration.ofSeconds(bufferDuration).toMillis();
        flushFrequency = Duration.ofSeconds(flushFrequencyDuration);
        this.bufferingStateStoreName = bufferingStateStoreName;
        this.scheduledTimeStoreName = scheduledTimeStoreName;
        this.queueStoreName = queueStoreName;
        this.bufferringEnabled = bufferringEnabled;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.context = context;
        bufferingStateStore = context.getStateStore(bufferingStateStoreName);
        queueStore = context.getStateStore(queueStoreName);
        scheduledTimeStore = context.getStateStore(scheduledTimeStoreName);
        context.schedule(flushFrequency, PunctuationType.WALL_CLOCK_TIME, this::flushKeys);
    }

    @SneakyThrows
    private void flushKeys(long now) {
        var iterator = queueStore.range(0L, System.currentTimeMillis());
        var keysToRemove = new ArrayList<Long>();

        while (iterator.hasNext()) {
            var next = iterator.next();
            keysToRemove.add(next.key);
            if (next.value != null) {
                var valueToForward = bufferingStateStore.delete(next.value);
                if (bufferringEnabled) {
                    // just avoid forwarding if buffering is disabled
                    context.forward(new Record<>(next.value, valueToForward, System.currentTimeMillis()));
                }
                scheduledTimeStore.delete(next.value);
            }
        }
        iterator.close();
        keysToRemove.forEach(queueStore::delete);
    }


    /*
    current behavior
     key if doesnt exist -> create a new scheduler and will wait till it goes over bufferTimeMs
     key if exist -> reuse the same scheduler which will be flushed (last value)
                after the original scheduled time (i.e. with bufferTimeMs)
    * */
    @Override
    public void process(Record<K, V> record) {
        if (bufferringEnabled) {
            var scheduledTime = scheduledTimeStore.get(record.key());
            /*
            To handle grace period - currently not needed
            if (scheduledTime == null || currentTime + gracePeriod > scheduledTime) {
                scheduledTime = currentTime + bufferTimeMs + gracePeriod;
                scheduledTimeStore.put(record.key(), scheduledTime);
            }
            * */
            if (scheduledTime == null) {
                scheduledTime = System.currentTimeMillis() + bufferTimeMs;
                scheduledTimeStore.put(record.key(), scheduledTime);
            }
            queueStore.put(scheduledTime, record.key());
            bufferingStateStore.put(record.key(), record.value());
        } else {
            context.forward(record);
        }
    }
}
