import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQProducer {
    public static void main(String[] args) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(Config.RABBITMQ_HOST);
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(Config.QUEUE_NAME, false, false, false, null);

                // Fetch and send data from BO1
                sendData(Config.BO1_DB_URL, "BO1", channel);

                // Fetch and send data from BO2
                sendData(Config.BO2_DB_URL, "BO2", channel);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendData(String dbUrl, String branch, Channel channel) throws Exception {
        String data = DatabaseUtils.fetchLatestSalesData(dbUrl);
        if (!data.isEmpty()) {
            String message = data;  // Prefix data with branch name
            channel.basicPublish("", Config.QUEUE_NAME, null, message.getBytes());
            System.out.println("Données envoyées depuis " + branch + " : " + data);
        } else {
            System.out.println("Aucune nouvelle donnée à envoyer depuis " + branch);
        }
    }
}
