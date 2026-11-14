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
public final class KafkaAgent {
    private KafkaAgent() {}
    public static void premain(String config, Instrumentation instrumentation) {
        try {
            var properties = new Properties();
            try (var in = Files.newBufferedReader(Path.of(config))) { properties.load(in); }
            var uri = URI.create(properties.getProperty("endpoint"));
            boolean local = List.of("127.0.0.1", "[::1]").contains(uri.getHost());
            if (!("https".equals(uri.getScheme()) || local && "http".equals(uri.getScheme())) ||
                uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null ||
                !"/monitoring/ingest".equals(uri.getPath())) throw new IllegalArgumentException();
            var tokenFile = Path.of(properties.getProperty("tokenFile"));
            var client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(4)).build();
            var pool = Executors.newSingleThreadScheduledExecutor(r -> {
                var t = new Thread(r, "kafdeck-metrics"); t.setDaemon(true); return t;
            });
            int interval = Integer.parseInt(properties.getProperty("intervalSeconds", "30"));
            if (interval < 10 || interval > 60) throw new IllegalArgumentException();
            pool.scheduleWithFixedDelay(() -> {
                try {
                    final String token = Files.readString(tokenFile).trim();
                    collect(body -> {
                        var request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(8))
                            .header("X-Kafdeck-Agent-Token", token).header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body)).build();
                        int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
                        if (status != 204) throw new java.io.IOException("HTTP " + status);
                    });
                } catch (Exception e) { System.err.println("Kafdeck monitoring capture failed: " + e.getClass().getSimpleName()); }
            }, 1, interval, TimeUnit.SECONDS);
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
    static void collect(Sink sink) throws Exception {
        var batch = new Batch(sink);
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        var names = new ArrayList<>(server.queryNames(null, null));
        names.sort(Comparator.comparing(ObjectName::getCanonicalName));
        for (var bean : names) {
            String domain = bean.getDomain();
            if (!(domain.startsWith("kafka.") || domain.equals("java.lang") || domain.equals("java.nio"))) continue;
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
                        if (value instanceof Number n) batch.add(metric, dimensions, n);
                        else if (value instanceof CompositeData obj) {
                            for (String k : obj.getCompositeType().keySet()) {
                                Object v = obj.get(k);
                                if (v instanceof Number n) batch.add(metric + "/" + k, dimensions, n);
                            }
                        }
                    } catch (javax.management.JMException | RuntimeException unavailable) { batch.partial = true; }
                }
            } catch (javax.management.JMException | RuntimeException disappeared) { batch.partial = true; }
        }
        batch.flush(true);
    }
    private static boolean numericType(String type) {
        return switch(type) {
            case "java.lang.Object","int","long","double","float","short","byte","java.lang.Number","java.lang.Integer","java.lang.Long",
                 "java.lang.Double","java.lang.Float","java.lang.Short","java.lang.Byte" -> true;
            default -> false;
        };
    }
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
