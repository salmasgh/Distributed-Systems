import com.rabbitmq.client.*;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class ReplicaV2 {
    private static final String WRITE_EXCHANGE = "lines_exchange";
    private static final String READ_REQUEST_EXCHANGE = "read_exchange";
    private static final String CLIENT_RESPONSE_QUEUE = "client_response_queue";

    public static void main(String[] argv) throws Exception {
        if (argv.length < 1) {
            System.err.println("Usage: java ReplicaV2 <replicaNumber>");
            System.exit(1);
        }
        String replicaNumber = argv[0];

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(WRITE_EXCHANGE, "fanout");
        channel.exchangeDeclare(READ_REQUEST_EXCHANGE, "fanout");

        String writeQueueName = "replica_queue_" + replicaNumber;
        try {
            channel.queueDelete(writeQueueName);
        } catch (IOException e) {
            System.out.println("Queue " + writeQueueName + " does not exist, continue...");
        }
        channel.queueDeclare(writeQueueName, false, false, false, null);
        channel.queueBind(writeQueueName, WRITE_EXCHANGE, "");

        String readRequestQueueName = "replica_read_queue_" + replicaNumber;
        try {
            channel.queueDelete(readRequestQueueName);
        } catch (IOException e) {
            System.out.println("Queue " + readRequestQueueName + " does not exist, continue...");
        }
        channel.queueDeclare(readRequestQueueName, false, false, false, null);
        channel.queueBind(readRequestQueueName, READ_REQUEST_EXCHANGE, "");

        System.out.println(" [*] Replica " + replicaNumber + " waiting for messages...");

        DeliverCallback writeCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [>] Replica " + replicaNumber + " received to write: '" + message + "'");
            writeToFile(replicaNumber, message);
        };

        DeliverCallback readCallback = (consumerTag, delivery) -> {
            String request = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [>] Replica " + replicaNumber + " received request: '" + request + "'");

            if (request.equals("Read Last")) {
                String lastLine = readLastLine(replicaNumber);
                if (lastLine != null) {
                    sendLineToClient(lastLine);
                }
            } else if (request.equals("Read All")) {
                List<String> allLines = readAllLines(replicaNumber);
                if (allLines != null) {
                    for (String line : allLines) {
                        sendLineToClient(line);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } 
                    }
                }
            }
        };

        channel.basicConsume(writeQueueName, true, writeCallback, consumerTag -> {});
        channel.basicConsume(readRequestQueueName, true, readCallback, consumerTag -> {});
    }

    private static void writeToFile(String replicaNumber, String line) {
        try {
            String directory = "replica" + replicaNumber;
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(directory));
            String filePath = directory + "/fichier.txt";

            FileWriter writer = new FileWriter(filePath, true);
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

    private static List<String> readAllLines(String replicaNumber) {
        try {
            String filePath = "replica" + replicaNumber + "/fichier.txt";
            return java.nio.file.Files.readAllLines(java.nio.file.Paths.get(filePath));
        } catch (IOException e) {
            System.err.println("Error reading file for replica " + replicaNumber);
            e.printStackTrace();
        }
        return null;
    }

    private static void sendLineToClient(String line) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.queueDeclare(CLIENT_RESPONSE_QUEUE, false, false, false, null);
                channel.basicPublish("", CLIENT_RESPONSE_QUEUE, null, line.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [<] Sent line to ClientReaderV2: '" + line + "'");
            }
        } catch (IOException | TimeoutException e) {
            System.err.println("Error sending line to ClientReaderV2");
            e.printStackTrace();
        }
    }
}
