package io.hstream.io.standalone;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.hstream.HRecord;
import io.hstream.io.*;
import io.hstream.io.impl.spec.ExtendedSpec;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


@Slf4j
public class StandAloneTaskRunner {
    @Parameters(
            commandNames = "spec",
            commandDescription = "get configuration specification"
    )
    static class SpecCmd {}

    @Parameters(
            commandNames = "check",
            commandDescription = "check the connector configuration before run the task"
    )
    static class CheckCmd {
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
    static class RunCmd {
        @Parameter(
                names = "--config",
                description = "configuration file path",
                required = true
        )
        String configPath;
    }

    SinkTaskContext ctx;
    // for json schema verifier only
    JsonNode cfgNode;
    HRecord cfg;
    SinkTask task;
    static ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    public void run(String[] args, SinkTask task, SinkTaskContext context) {
        log.info("task runner started");
        this.task = task;
        this.ctx = context;
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
                if (task instanceof SourceTask) {
                    System.out.println(task.spec());
                } else {
                    System.out.println(ExtendedSpec.merge(task.spec()));
                }
                System.out.flush();
                break;
            case "check":
                parseConfig(checkCmd.configPath);
                var result = check(task);
                System.out.println(mapper.writeValueAsString(result));
                System.out.println("CONNECTOR CHECK FAILED!");
                System.out.flush();
                break;
            case "run":
                parseConfig(runCmd.configPath);
                var checkResult = check(task);
                if (!checkResult.getResult()) {
                    log.error("connector check failed, result:{}", mapper.writeValueAsString(checkResult));
                    System.exit(1);
                }
                var connectorConfig = cfg.getHRecord("connector");
                try {
                    ctx.init(cfg, new NullKvStore());
                    task.run(connectorConfig, ctx);
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
        return task.check(cfg.getHRecord("connector"));
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
}