package upload.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/** Console prompts for the spec inputs (p, c, q). Enter keeps the default. */
public final class UserInput {

    private UserInput() {}

    private static final BufferedReader IN = new BufferedReader(new InputStreamReader(System.in));

    /**
     * Asks for a whole number of 1 or more, e.g. "Number of producer threads (p) [3]: ".
     * Blank input keeps the default; invalid input asks again. If there is no console input
     * (stdin closed, e.g. some IDE or plugin launches), the default is used.
     */
    public static synchronized int askPositiveInt(String label, int defaultValue) {
        while (true) {
            System.out.printf("%s [%d]: ", label, defaultValue);
            System.out.flush();
            String line;
            try {
                line = IN.readLine();
            } catch (IOException e) {
                line = null;
            }
            if (line == null) {
                System.out.println();
                System.out.println("  (no console input available, using " + defaultValue + ")");
                return defaultValue;
            }
            line = line.trim();
            if (line.isEmpty()) {
                return defaultValue;
            }
            try {
                int value = Integer.parseInt(line);
                if (value >= 1) {
                    return value;
                }
            } catch (NumberFormatException ignored) {
                // fall through
            }
            System.out.println("  Please enter a whole number of 1 or more (or press Enter for " + defaultValue + ").");
        }
    }

    /** Parses a "--name=value" option as a positive integer, with a message naming the option. */
    public static int parsePositiveOption(String option, String label) {
        String value = option.substring(option.indexOf('=') + 1).trim();
        try {
            int parsed = Integer.parseInt(value);
            if (parsed >= 1) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
            // fall through
        }
        throw new IllegalArgumentException(label + " must be a whole number of 1 or more, got '" + value + "'.");
    }
}
