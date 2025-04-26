import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ClientReader {
    private static final String EXCHANGE_NAME = "read_exchange";
    private static final String RESPONSE_QUEUE = "client_response_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Déclare l'exchange pour envoyer la demande
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        // Déclare la file de réponse pour écouter les réponses
        channel.queueDeclare(RESPONSE_QUEUE, false, false, false, null);

        // Envoyer le message "Read Last" vers l'exchange
        String requestMessage = "Read Last";
        channel.basicPublish(EXCHANGE_NAME, "", null, requestMessage.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent request '" + requestMessage + "'");

        // Maintenant, consommer la première réponse qui arrive
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String response = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [.] Received last line: " + response);
            // Après réception d'une réponse, fermer tout
            try {
                channel.close();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            connection.close();
            System.exit(0);
        };

        channel.basicConsume(RESPONSE_QUEUE, true, deliverCallback, consumerTag -> {});
    }
}