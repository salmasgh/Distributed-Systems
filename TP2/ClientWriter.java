import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ClientWriter {
    private static final String EXCHANGE_NAME = "lines_exchange";

    public static void main(String[] argv) throws Exception {
        // Setup the connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); 
        try (Connection connection = factory.newConnection(); 
             Channel channel = connection.createChannel()) {

            // Déclarer un exchange (type fanout pour tout envoyer à plusieurs files)
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            // Exemple d'envoi de lignes
            String[] lines = {
                // "Texte 1...", 
                // "Texte 2...", 
                "Texte 4...", 
                "Texte 7..."
            };

            for (String line : lines) {
                channel.basicPublish(EXCHANGE_NAME, "", null, line.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + line + "'");
            }
        }
    }
}