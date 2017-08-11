package com.github.vitalibo.a3c.provisioner.model.transform;

import com.github.vitalibo.a3c.provisioner.util.Jackson;
import freemarker.template.Template;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.io.CharArrayWriter;
import java.io.Writer;
import java.util.Map;

@AllArgsConstructor
public class QueryStringTranslator<T> {

    private final Template freemarkerTemplate;

    @SneakyThrows
    public String from(T o) {
        Writer out = new CharArrayWriter();
        freemarkerTemplate.process(
            Jackson.convertValue(o, Map.class), out);
        return out.toString();
    }

}
