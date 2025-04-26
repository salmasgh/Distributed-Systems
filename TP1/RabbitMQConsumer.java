import com.rabbitmq.client.*;

public class RabbitMQConsumer {
    public static void main(String[] args) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(Config.RABBITMQ_HOST);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
    
            channel.queueDeclare(Config.QUEUE_NAME, false, false, false, null);
            System.out.println("En attente des messages...");
    
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String data = new String(delivery.getBody(), "UTF-8");
                System.out.println("Données reçues : " + data);
    
                // Insert data into the database
                DatabaseUtils.insertSalesData(Config.HO_DB_URL, data);
                System.out.println("Données insérées dans HO_DB");
            };
    
            channel.basicConsume(Config.QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        } catch (Exception e) {
            System.out.println("Error in main: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
