import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ClientReaderV2 {
    private static final String EXCHANGE_NAME = "read_exchange"; // le même échange pour demander
    private static final String RESPONSE_QUEUE = "client_response_queue"; // où recevoir les réponses

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Déclare exchange pour envoyer la demande
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        // Déclare la file pour recevoir les réponses
        channel.queueDeclare(RESPONSE_QUEUE, false, false, false, null);

        // Envoyer le message "Read All"
        String requestMessage = "Read All";
        channel.basicPublish(EXCHANGE_NAME, "", null, requestMessage.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent request '" + requestMessage + "'");

        // Collecte des réponses
        List<String> allLines = new ArrayList<>();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String line = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [.] Received line: " + line);
            allLines.add(line);
        };

        channel.basicConsume(RESPONSE_QUEUE, true, deliverCallback, consumerTag -> {});

        // Attendre un peu pour recevoir toutes les lignes (ex: 5 secondes)
        Thread.sleep(5000);
        // Traiter les majorités
        Map<String, Integer> countMap = new HashMap<>();
        for (String line : allLines) {
            countMap.put(line, countMap.getOrDefault(line, 0) + 1);
        }

        System.out.println("\n [*] Lines appearing in majority:");
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() >= 2) { // majorité : au moins 2 votes
                System.out.println(" -> " + entry.getKey());
            }
        }

        channel.close();
        connection.close();
    }
}