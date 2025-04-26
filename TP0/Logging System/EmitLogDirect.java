import com.rabbitmq.client.*;
import com.rabbitmq.client.BuiltinExchangeType;

public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String severity = getSeverity(argv);
            String message = getMessage(argv);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }

    private static String getSeverity(String[] argv) {
        if (argv.length < 1) {
            return "info";
        }
        return argv[0];
    }

    private static String getMessage(String[] argv) {
        if (argv.length < 2) {
            return "Hello World!";
        }
        return String.join(" ", java.util.Arrays.copyOfRange(argv, 1, argv.length));
    }
}