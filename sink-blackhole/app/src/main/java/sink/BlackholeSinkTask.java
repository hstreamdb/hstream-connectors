package sink;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.Utils;
import io.hstream.io.impl.SinkTaskContextImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlackholeSinkTask implements SinkTask {
    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        ctx.handle((stream, records) -> {
            log.debug("bulkWrite records:{} from stream:{}", records, stream);
        });
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @Override
    public void stop() {}

    public static void main(String[] args) {
        new TaskRunner().run(args, new BlackholeSinkTask(), new SinkTaskContextImpl());
    }
}
