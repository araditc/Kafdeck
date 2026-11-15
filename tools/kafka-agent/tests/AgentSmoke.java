import java.lang.management.ManagementFactory;
import javax.management.ObjectName;

public class AgentSmoke {
    public interface ProbeMBean {
        int getValue();
        int getBroken();
    }

    public static class Probe implements ProbeMBean {
        public int getValue() { return 73; }
        public int getBroken() { if(Boolean.getBoolean("probe.failure")) throw new IllegalStateException(); return 11; }
    }
    public interface MemoryPoolMBean {
        boolean isUsageThresholdSupported();
        long getUsageThreshold();
        long getUsageThresholdCount();
    }
    public static class MemoryPool implements MemoryPoolMBean {
        public boolean isUsageThresholdSupported() { return false; }
        public long getUsageThreshold() { throw new AssertionError("Unsupported getter must never be invoked"); }
        public long getUsageThresholdCount() { throw new AssertionError("Unsupported getter must never be invoked"); }
    }

    public static void main(String[] args) throws Exception {
        ManagementFactory.getPlatformMBeanServer().registerMBean(
            new Probe(), new ObjectName("kafka.test:type=Probe,name=Compatibility"));
        ManagementFactory.getPlatformMBeanServer().registerMBean(
            new MemoryPool(),new ObjectName("java.lang:type=MemoryPool,name=Kafdeck Capability Probe"));
        for (String topic : new String[]{"orders","other"})
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                new Probe(), new ObjectName("kafka.test:type=Probe,name=Partition,topic="+topic+",partition=0"));
        Thread.sleep(60_000);
    }
}
