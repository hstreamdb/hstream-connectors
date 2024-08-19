package source.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.SourceTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SourceTaskContextImpl;

public class MysqlSourceTask extends RdbSourceTask {
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        var dbname = cfg.getString("database");
        var table = cfg.getString("table");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("database.include.list", dbname);
        props.setProperty("table.include.list", table);
        if (table.split("\\.").length == 1) {
            props.setProperty("table.include.list", dbname + "." + table);
        }
        if(cfg.contains("serverTimeZone")){
            props.setProperty("database.connectionTimeZone ", cfg.getString("serverTimeZone"));
        }
        super.run(cfg, ctx);
    }

    @Override
    public JsonNode spec() {
        return io.hstream.io.Utils.getSpec(this, "/source_mysql_spec.json");
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new MysqlSourceTask(), ctx);
    }
}
