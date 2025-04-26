import com.rabbitmq.client.*;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Replica {
    private static final String WRITE_EXCHANGE = "lines_exchange"; // pour recevoir les lignes à écrire
    private static final String READ_REQUEST_EXCHANGE = "read_exchange"; // pour recevoir les requêtes Read Last
    private static final String CLIENT_RESPONSE_QUEUE = "client_response_queue"; // où envoyer la réponse "last line"

    public static void main(String[] argv) throws Exception {
        if (argv.length < 1) {
            System.err.println("Usage: java Replica <replicaNumber>");
            System.exit(1);
        }
        String replicaNumber = argv[0];

        // Setup connection RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Déclarer les exchanges
        channel.exchangeDeclare(WRITE_EXCHANGE, "fanout");
        channel.exchangeDeclare(READ_REQUEST_EXCHANGE, "fanout");

        // Déclarer la queue pour recevoir les écritures
        String writeQueueName = "replicaqueue" + replicaNumber;
        channel.queueDeclare(writeQueueName, false, false, true, null);
        channel.queueBind(writeQueueName, WRITE_EXCHANGE, "");
        
        // Déclarer la queue pour recevoir les requêtes "Read Last"
        String readRequestQueueName = "replica_readqueue" + replicaNumber;
        channel.queueDeclare(readRequestQueueName, false, false, false, null);
        channel.queueBind(readRequestQueueName, READ_REQUEST_EXCHANGE, "");

        System.out.println(" [*] Replica " + replicaNumber + " waiting for messages...");

        // Consumer pour les lignes à écrire
        DeliverCallback writeCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [>] Replica " + replicaNumber + " received to write: '" + message + "'");
            writeToFile(replicaNumber, message);
        };

        // Consumer pour les requêtes "Read Last"
        DeliverCallback readLastCallback = (consumerTag, delivery) -> {
            String request = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [>] Replica " + replicaNumber + " received request: '" + request + "'");

            if (request.equals("Read Last")) {
                String lastLine = readLastLine(replicaNumber);
                if (lastLine != null) {
                    sendLastLine(lastLine);
                }
            }
        };

        // Consommer les deux types de messages
        channel.basicConsume(writeQueueName, true, writeCallback, consumerTag -> {});
        channel.basicConsume(readRequestQueueName, true, readLastCallback, consumerTag -> {});
    }

    private static void writeToFile(String replicaNumber, String line) {
        try {
            // Création du dossier si besoin
            String directory = "replica" + replicaNumber;
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(directory));
            String filePath = directory + "/fichier.txt";

            FileWriter writer = new FileWriter(filePath, true); // true = append
            writer.write(line + "\n");
            writer.close();
        } catch (IOException e) {
            System.err.println("Error writing to file for replica " + replicaNumber);
            e.printStackTrace();
        }
    }

    private static String readLastLine(String replicaNumber) {
        try {
            String filePath = "replica" + replicaNumber + "/fichier.txt";
            List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(filePath));
            if (!lines.isEmpty()) {
                return lines.get(lines.size() - 1);
            }
        } catch (IOException e) {
            System.err.println("Error reading file for replica " + replicaNumber);
            e.printStackTrace();
        }
        return null;
    }
    
    private static void sendLastLine(String lastLine) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.queueDeclare(CLIENT_RESPONSE_QUEUE, false, false, false, null);
                channel.basicPublish("", CLIENT_RESPONSE_QUEUE, null, lastLine.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [<] Sent last line to ClientReader: '" + lastLine + "'");
            }
        } catch (IOException | TimeoutException e) {
            System.err.println("Error sending last line to ClientReader");
            e.printStackTrace();
        }
    }
}