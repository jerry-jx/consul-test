package org.windwant.consul;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;

/**
 * Unit test for simple TestDubboService.
 */
public class TestConsulMgrTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestConsulMgrTest(String testName)
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( TestConsulMgrTest.class );
    }

    /**
     * Rigourous Test :-)
     */

    public void testPutKV(){
        ConsulMgr.put("student", "lilei");
    }

    public void testTestConsulService() throws IOException {
        ConsulMgr.getHealthService("tomcatSvr");
    }

    public void testRegister(){
        ConsulMgr.registerService();
    }

}
