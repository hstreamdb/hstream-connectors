package io.hstream.io;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.hstream.HRecord;
import io.hstream.io.impl.ChannelKvStore;
import io.hstream.io.impl.StdioChannel;
import io.hstream.io.impl.spec.ExtendedSpec;
import io.hstream.io.internal.Channel;

import java.awt.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.function.Supplier;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Parameters(
        commandNames = "spec",
        commandDescription = "get configuration specification"
)
class SpecCmd {}

@Parameters(
    commandNames = "check",
    commandDescription = "check the connector configuration before run the task"
)
class CheckCmd {
    @Parameter(
        names = "--config",
        description = "configuration file path",
        required = true
    )
    String configPath;
}

@Parameters(
    commandNames = "run",
    commandDescription = "run the task"
)
class RunCmd {
    @Parameter(
        names = "--config",
        description = "configuration file path",
        required = true
    )
    String configPath;
}

@Slf4j
public class TaskRunner {
    TaskContext ctx;
    // for json schema verifier only
    JsonNode cfgNode;
    HRecord cfg;
    Task task;
    Channel channel;
    Rpc rpc;
    KvStore kv;
    static ObjectMapper mapper = new ObjectMapper();
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

    @SneakyThrows
    public void run(String[] args, Task task, TaskContext taskContext) {
        log.info("task runner started");
        this.task = task;
        this.ctx = taskContext;
        this.channel = new StdioChannel();
        this.rpc = new Rpc(channel);
        this.kv = new ChannelKvStore(rpc);
        var checkCmd = new CheckCmd();
        var runCmd = new RunCmd();
        var jc = JCommander.newBuilder()
                .addCommand(new SpecCmd())
                .addCommand(checkCmd)
                .addCommand(runCmd)
                .build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.error(e.getMessage());
            jc.usage();
            System.exit(1);
        }
        switch (jc.getParsedCommand()) {
            case "spec":
                System.out.println(ExtendedSpec.merge(task.spec()));
                System.out.flush();
                break;
            case "check":
                parseConfig(checkCmd.configPath);
                var result = check(task);
                System.out.println(mapper.writeValueAsString(result));
                System.out.flush();
                break;
            case "run":
                parseConfig(runCmd.configPath);
                executor.execute(this::recvCmd);
                executor.scheduleAtFixedRate(this::report, 3, 3, TimeUnit.SECONDS);
                var connectorConfig = cfg.getHRecord("connector");
                try {
                    ctx.init(cfg, kv);
                    if (task instanceof SourceTask) {
                        var st = (SourceTask) task;
                        var stc = (SourceTaskContext) taskContext;
                        st.run(connectorConfig, stc);
                    } else {
                        var st = (SinkTask) task;
                        var stc = (SinkTaskContext) taskContext;
                        st.run(connectorConfig, stc);
                    }
                } catch (Throwable e) {
                    log.info("unexpected error when running connector:{}", e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                break;
            default:
                log.error("invalid cmd:{}", jc.getParsedCommand());
        }
        System.exit(0);
    }

    CheckResult check(Task task) {
        // check schema
        var schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(task.spec());
        var errors = schema.validate(cfgNode.get("connector"));
        if (errors.size() != 0) {
            return CheckResult.builder()
                    .result(false)
                    .type(CheckResult.CheckResultType.CONFIG)
                    .message("invalid config")
                    .detail(mapper.valueToTree(errors))
                    .build();
        }
        // task check
        return task.check(cfg);
    }

    void parseConfig(String cfgPath) {
        log.info("config file path:{}", cfgPath);
        try {
            var cfgText = Files.readString(Paths.get(cfgPath));
            this.cfg = HRecord.newBuilder().merge(cfgText).build();
            this.cfgNode = new ObjectMapper().readTree(cfgText);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void recvCmd() {
        log.info("receiving commands");
        channel.handle(msg -> {
            var cmdType = msg.get("name").asText();
            if (cmdType.equals("stop")) {
                Utils.runWithTimeout(1, this::stop);
                System.exit(0);
            }
        });
    }

    public void report() {
        log.info("reporting task information");
        try {
            var reportMessage = ctx.getReportMessage();
            if (task instanceof SourceTask) {
                reportMessage.setOffsets(((SourceTask) task).getOffsets());
            }
            rpc.report(reportMessage).get(5, TimeUnit.SECONDS);
        } catch (Throwable e) {
            log.info("report exited:{}", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void stop() {
        try {
            log.info("stopping runner");
            if (task instanceof SourceTask) {
                task.stop();
                log.info("stopped source task");
                ctx.close();
                log.info("stopped source context");
            } else {
                ctx.close();
                log.info("stopped sink context");
                task.stop();
                log.info("stopped sink task");
            }
            log.info("stopped runner");
            kv.close();
            channel.close();
        } catch (InterruptedException ignored) {
            System.exit(1);
        }
    }
}