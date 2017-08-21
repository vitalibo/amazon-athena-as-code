package com.github.vitalibo.a3c.provisioner.model.transform;

import com.github.vitalibo.a3c.provisioner.Factory;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class QueryStringTranslatorTest {

    private Configuration cfg;

    @BeforeClass
    public void init() {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
        cfg.setClassForTemplateLoading(Factory.class, "/");
        cfg.setDefaultEncoding(StandardCharsets.UTF_8.name());
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        this.cfg = cfg;
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            {"CreateDatabaseQuery"}, {"DropDatabaseQuery"}, {"UpdateDatabasePropertiesQuery"}
        };
    }

    @Test(dataProvider = "samples")
    public void testFromDatabase(String name) throws IOException {
        testFrom(name, Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Database/Request.json"), DatabaseProperties.class));
    }

    @DataProvider
    public Object[][] samplesTables() {
        return new Object[][]{
            {"CreateTableQuery"}, {"DropTableQuery"}
        };
    }

    @Test(dataProvider = "samplesTables")
    public void testFromTable(String name) throws IOException {
        testFrom(name, Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Table/Request.json"), TableProperties.class));
    }

    private void testFrom(String name, Object o) throws IOException {
        QueryStringTranslator<Object> translator = new QueryStringTranslator<>(
            cfg.getTemplate(String.format("/Template/%s.ftl", name)));

        String actual = translator.from(o);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual,
            TestHelper.resourceAsString(String.format("/Query/%s.sql", name)));
    }

}