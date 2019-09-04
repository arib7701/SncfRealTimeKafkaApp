package ribot.amandine.kafka.app.transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import ribot.amandine.kafka.app.Disruption;
import ribot.amandine.kafka.app.KeyDisruption;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DiscardDuplicatesTransformer implements Transformer<KeyDisruption, Disruption, KeyValue<KeyDisruption, Disruption>> {

    private KeyValueStore<KeyDisruption, Disruption> stateStore;
    private ProcessorContext context;
    private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss");

    @Override
    public void init(ProcessorContext processorContext) {
        this.stateStore = (KeyValueStore<KeyDisruption, Disruption>) processorContext.getStateStore("disruptions-store");
        this.context = processorContext;
    }

    @Override
    public KeyValue<KeyDisruption, Disruption> transform(KeyDisruption key, Disruption disruptionFromTopic) {

        Disruption disruptionFromStateStore = this.stateStore.get(key);

        try {
            if(disruptionFromStateStore == null || moreRecentUpdate(disruptionFromTopic, disruptionFromStateStore)) {
                this.stateStore.put(key, disruptionFromTopic);
                this.context.forward(key, disruptionFromTopic);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    private boolean moreRecentUpdate(Disruption disruptionFromTopic, Disruption disruptionFromStateStore) throws ParseException {

        Date updateFromTopic = dateFormatter.parse(disruptionFromTopic.getUpdatedAt().toString());
        Date updateFromStateStore = dateFormatter.parse(disruptionFromStateStore.getUpdatedAt().toString());

        return (updateFromTopic.after(updateFromStateStore));
    }

    @Override
    public void close() {

    }
}
