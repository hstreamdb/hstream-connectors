package sink;

import io.hstream.HRecord;
import lombok.Builder;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Builder
public class ExtraTimeField {
    String fieldName;
    String timeFormat;

    public String getFieldName() {
        return fieldName;
    }

    public String getValue() {
        return DateTimeFormatter.ofPattern(timeFormat)
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());
    }

    static public ExtraTimeField fromConfig(HRecord cfg) {
        if (cfg.contains("addExtraDateTimeField") && cfg.getBoolean("addExtraDateTimeField")) {
            return ExtraTimeField.builder()
                    .fieldName(cfg.getString("extraDateTimeFieldName"))
                    .timeFormat(cfg.getString("extraDateTimeFieldFormat"))
                    .build();
        }
        return null;
    }
}
