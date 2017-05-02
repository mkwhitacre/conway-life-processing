package com.mkwhitacre.conway.crunch;


import org.apache.crunch.test.TemporaryPath;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ConwayToolTest {

    private TemporaryPath tempPath;
    private Configuration config;

    @Before
    public void setup() throws Throwable {
        tempPath = new TemporaryPath();
        tempPath.create();

        config = tempPath.getDefaultConfiguration();
    }

    @After
    public void cleanup(){
        tempPath.delete();
    }

    @Test
    public void run() throws Exception {
        ConwayTool tool = new ConwayTool();
        tool.setConf(config);
        assertThat(tool.run(new String[0]), is(0));
    }



}
