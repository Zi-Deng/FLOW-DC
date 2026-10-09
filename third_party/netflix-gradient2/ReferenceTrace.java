import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.measurement.Measurement;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Invokes unchanged upstream code. Reflection exposes state, not a second algorithm. */
public final class ReferenceTrace {
    public static void main(String[] args) throws Exception {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        Field estimated = Gradient2Limit.class.getDeclaredField("estimatedLimit");
        Field average = Gradient2Limit.class.getDeclaredField("longRtt");
        estimated.setAccessible(true);
        average.setAccessible(true);
        Gradient2Limit limit = null;
        String scenario = null;
        int index = 0;
        String line;
        System.out.println("scenario,index,delay_ns,inflight,did_drop,initial_limit,min_limit,max_limit,queue_size,smoothing,long_window,rtt_tolerance,limit,estimated_limit,last_delay_ns,long_delay_ns");
        while ((line = input.readLine()) != null) {
            String[] row = line.split(",", -1);
            if (row.length != 12) throw new IllegalArgumentException("Expected 12 input fields");
            if (!row[0].equals(scenario)) {
                scenario = row[0]; index = 0;
                limit = Gradient2Limit.newBuilder().initialLimit(Integer.parseInt(row[5]))
                    .minLimit(Integer.parseInt(row[6])).maxConcurrency(Integer.parseInt(row[7]))
                    .queueSize(Integer.parseInt(row[8])).smoothing(Double.parseDouble(row[9]))
                    .longWindow(Integer.parseInt(row[10])).rttTolerance(Double.parseDouble(row[11])).build();
            }
            if (Integer.parseInt(row[1]) != index++) throw new IllegalArgumentException("Nonsequential index");
            long delay = Long.parseLong(row[2]);
            int inflight = Integer.parseInt(row[3]);
            if (delay <= 0 || inflight < 0 || !(row[4].equals("true") || row[4].equals("false")))
                throw new IllegalArgumentException("Invalid observation");
            limit.onSample(0L, delay, inflight, Boolean.parseBoolean(row[4]));
            System.out.printf(Locale.ROOT, "%s,%d,%.17g,%d,%.17g%n", line, limit.getLimit(),
                estimated.getDouble(limit), limit.getLastRtt(TimeUnit.NANOSECONDS),
                ((Measurement) average.get(limit)).get().doubleValue());
        }
    }
}
