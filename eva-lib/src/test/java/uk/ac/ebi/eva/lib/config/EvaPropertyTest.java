package uk.ac.ebi.eva.lib.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.lib.configuration.EvaPropertyTestConfiguration;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {EvaPropertyTestConfiguration.class})
@SpringBootTest(classes = {EvaPropertyTestConfiguration.class})
@EnableConfigurationProperties
public class EvaPropertyTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private EvaProperty evaProperty;

    @Test
    public void testEvaPropertyAutowiring() {
        assertNotNull(evaProperty);
        assertNotNull(evaProperty.getMongo());
    }

}