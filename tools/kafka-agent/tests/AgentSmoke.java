import java.lang.management.ManagementFactory;
import javax.management.ObjectName;

public class AgentSmoke {
    public interface ProbeMBean {
        int getValue();
    }

    public static class Probe implements ProbeMBean {
        public int getValue() { return 73; }
    }

    public static void main(String[] args) throws Exception {
        ManagementFactory.getPlatformMBeanServer().registerMBean(
            new Probe(), new ObjectName("kafka.test:type=Probe,name=Compatibility"));
        for (String topic : new String[]{"orders","other"})
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                new Probe(), new ObjectName("kafka.test:type=Probe,name=Partition,topic="+topic+",partition=0"));
        Thread.sleep(60_000);
    }
}
