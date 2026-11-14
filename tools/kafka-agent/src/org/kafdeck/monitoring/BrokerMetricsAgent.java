package org.kafdeck.monitoring;

import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

/** Loaded by Kafka itself. No JMX remote port, HTTP listener or external collector. */
public final class BrokerMetricsAgent {
    private BrokerMetricsAgent() {}
    private static boolean running;
    public static void agentmain(String config, Instrumentation instrumentation) {
        premain(config,instrumentation);
    }
    public static synchronized void premain(String config, Instrumentation instrumentation) {
        if(running) return;
        try {
            // When upgrading an already attached legacy agent, collect only its missing topic dimensions.
            boolean topicOnly = config != null && config.startsWith("topics-only:");
            if (topicOnly) config = config.substring("topics-only:".length());
            var properties = new Properties();
            try (var in = Files.newBufferedReader(Path.of(config))) { properties.load(in); }
            var uri = URI.create(properties.getProperty("endpoint"));
            boolean local = List.of("127.0.0.1", "[::1]").contains(uri.getHost());
            if (!("https".equals(uri.getScheme()) || local && "http".equals(uri.getScheme())) ||
                uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null ||
                !"/monitoring/ingest".equals(uri.getPath())) throw new IllegalArgumentException();
            var tokenFile = Path.of(properties.getProperty("tokenFile"));
            var builder = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(4));
            if (properties.containsKey("caFile")) {
                var trust = KeyStore.getInstance(KeyStore.getDefaultType()); trust.load(null,null);
                try (var pem=Files.newInputStream(Path.of(properties.getProperty("caFile")))) {
                    trust.setCertificateEntry("kafdeck",CertificateFactory.getInstance("X.509").generateCertificate(pem));
                }
                var managers=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); managers.init(trust);
                var tls=SSLContext.getInstance("TLS"); tls.init(null, managers.getTrustManagers(), null);
                builder.sslContext(tls);
            }
            var client = builder.build();
            var pool = Executors.newSingleThreadScheduledExecutor(r -> {
                var t = new Thread(r, topicOnly ? "kafdeck-topic-metrics" : "kafdeck-metrics"); t.setDaemon(true); return t;
            });
            int interval = Integer.parseInt(properties.getProperty("intervalSeconds", "30"));
            if (interval < 10 || interval > 60) throw new IllegalArgumentException();
            boolean perPartition=Boolean.parseBoolean(properties.getProperty("includeTopicPartitions","true"));
            class Capture implements Runnable {
                int delay=interval;
                java.util.Set<String> topics = null;
                public void run() {
                try {
                    final String token = Files.readString(tokenFile).trim();
                    collect(body -> {
                        var request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(8))
                            .header("X-Kafdeck-Agent-Token", token).header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body));
                        if (topicOnly) request.header("X-Kafdeck-Topic-Only","1");
                        var response=client.send(request.build(), HttpResponse.BodyHandlers.discarding());
                        if (response.statusCode()!=204) throw new java.io.IOException("HTTP "+response.statusCode());
                        var setting=response.headers().firstValue("X-Kafdeck-Interval-Seconds");
                        if(setting.isPresent()) {
                            int seconds=Integer.parseInt(setting.get());
                            if(seconds>=10 && seconds<=60) delay=seconds;
                        }
                        var selection=response.headers().firstValue("X-Kafdeck-Topics").orElse("");
                        if (selection.startsWith("v1:")) {
                            var list=selection.substring(3);
                            topics=list.isEmpty() ? java.util.Set.of() : java.util.Set.copyOf(List.of(list.split(",")));
                        }
                    },perPartition,topicOnly,topics);
                } catch (Exception e) { System.err.println("Kafdeck monitoring capture failed: " + e.getClass().getSimpleName()); }
                finally { pool.schedule(this,delay,TimeUnit.SECONDS); }
                }
            }
            pool.schedule(new Capture(),1,TimeUnit.SECONDS);
            running=true;
        } catch (Exception e) { System.err.println("Kafdeck monitoring configuration invalid: " + e.getClass().getSimpleName()); }
    }

    @FunctionalInterface interface Sink { void send(String body) throws Exception; }
    private static final class Batch {
        final Sink sink;
        final String cycle = java.util.UUID.randomUUID().toString();
        int part;
        Batch(Sink sink) { this.sink = sink; }
        final List<String> rows = new ArrayList<>();
        boolean partial;
        int bytes;
        void flush(boolean last) throws Exception {
            sink.send("{\"partial\":" + partial + ",\"cycle\":" + quote(cycle) + ",\"part\":" + part +
                ",\"last\":" + last + ",\"samples\":[" + String.join(",",rows) + "]}");
            part++; rows.clear(); bytes=0;
        }
        void add(String metric, String labels, Number number) throws Exception {
            double value = number.doubleValue();
            if (!Double.isFinite(value)) return;
            String row = "{\"metric\":" + quote(metric) + ",\"labels\":" + labels + ",\"value\":" + value + "}";
            int length = row.getBytes(StandardCharsets.UTF_8).length + 1;
            if (rows.size() >= 8192 || bytes + length > 1950000) flush(false);
            rows.add(row); bytes += length;
        }
    }
    private static long lastGcMillis=-1, lastTick;
    private static void collectJvm(Batch batch) throws Exception {
        var os=ManagementFactory.getOperatingSystemMXBean();
        if(os instanceof com.sun.management.OperatingSystemMXBean) {
            double load=((com.sun.management.OperatingSystemMXBean)os).getProcessCpuLoad();
            if(load>=0 && load<=1) batch.add("kafdeck.jvm/cpu_percent","{}",load*100);
        }
        long tick=System.nanoTime();
        var times=ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean->bean.getCollectionTime()).toArray();
        if(times.length>0 && java.util.Arrays.stream(times).allMatch(v->v>=0)) {
            long total=java.util.Arrays.stream(times).sum();
            if(lastGcMillis>=0 && total>=lastGcMillis && tick>lastTick)
                batch.add("kafdeck.jvm/gc_time_percent","{}",(total-lastGcMillis)*100_000_000d/(tick-lastTick));
            lastGcMillis=total; lastTick=tick;
        } else { lastGcMillis=-1; }
    }
    static void collect(Sink sink,boolean perPartition,boolean topicOnly,java.util.Set<String> topics) throws Exception {
        var batch = new Batch(sink);
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        var names = new ArrayList<ObjectName>();
        if (topicOnly) {
            if (topics != null) for (var topic : topics)
                names.addAll(server.queryNames(new ObjectName("kafka.*:topic="+topic+",*"), null));
        } else {
            collectJvm(batch);
            names.addAll(server.queryNames(null, null));
        }
        names.sort(Comparator.comparing(ObjectName::getCanonicalName));
        for (var bean : names) {
            String domain = bean.getDomain();
            if (!(domain.startsWith("kafka.") || domain.equals("java.lang") || domain.equals("java.nio"))) continue;
            String topic=bean.getKeyProperty("topic");
            if (topic != null || bean.getKeyProperty("partition") != null) {
                boolean included = topics == null ? perPartition : topic != null && topics.contains(topic);
                if (!included) continue;
            }
            if (batch.part >= 63) { batch.partial = true; break; }
            try {
                var labels = new TreeMap<>(bean.getKeyPropertyList());
                // Keep original JMX identity. Only dimension values from ObjectName, never attribute text.
                String type = labels.remove("type");
                String name = labels.remove("name");
                if (type == null) continue;
                String prefix = domain + ":" + type + (name == null ? "" : "/" + name);
                var fields = new ArrayList<String>();
                for (var entry : labels.entrySet()) fields.add(quote(entry.getKey()) + ":" + quote(entry.getValue()));
                String dimensions = "{" + String.join(",", fields) + "}";
                for (var attribute : server.getMBeanInfo(bean).getAttributes()) {
                    if (!attribute.isReadable()) continue;
                    String field = attribute.getName();
                    String attrType = attribute.getType();
                    if (!numericType(attrType) && !attrType.equals("javax.management.openmbean.CompositeData")) continue;
                    try {
                        Object value = server.getAttribute(bean, field);
                        String metric = prefix + "/" + field;
                        if (value instanceof Number) batch.add(metric, dimensions, (Number)value);
                        else if (value instanceof CompositeData) {
                            var obj=(CompositeData)value;
                            for (String k : obj.getCompositeType().keySet()) {
                                Object v = obj.get(k);
                                if (v instanceof Number) batch.add(metric + "/" + k, dimensions, (Number)v);
                            }
                        }
                    } catch (javax.management.JMException | RuntimeException unavailable) { batch.partial = true; }
                }
            } catch (javax.management.JMException | RuntimeException disappeared) { batch.partial = true; }
        }
        batch.flush(true);
    }
    private static boolean numericType(String type) {
        return NUMERIC_TYPES.contains(type);
    }
    private static final java.util.Set<String> NUMERIC_TYPES=java.util.Set.of("java.lang.Object","int","long","double","float","short",
        "byte","java.lang.Number","java.lang.Integer","java.lang.Long","java.lang.Double","java.lang.Float","java.lang.Short","java.lang.Byte");
    private static String quote(String s) {
        var out = new StringBuilder("\"");
        for (int i=0;i<s.length();i++) {
            char c=s.charAt(i);
            if (c=='\"' || c=='\\') out.append('\\').append(c);
            else if (c<32) out.append(String.format("\\u%04x",(int)c));
            else out.append(c);
        }
        return out.append('\"').toString();
    }
}
