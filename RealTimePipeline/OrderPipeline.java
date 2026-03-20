import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class OrderPipeline {

    
    static BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {

    
        Thread producer = new Thread(() -> produceOrders());

        
        Thread consumer = new Thread(() -> consumeOrders());

        producer.start();
        consumer.start();
    }

    public static void produceOrders() {
        String[] products = {"Phone", "Laptop", "Tablet"};
        Random rand = new Random();
        int orderId = 1;

        try {
            while (true) {
                String product = products[rand.nextInt(products.length)];
                int amount = (rand.nextInt(5) + 1) * 10000;
                String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        .format(new java.util.Date());

                String order = orderId + "," + product + "," + amount + "," + timestamp;

                queue.put(order);
                System.out.println("Produced: " + order);

                orderId++;
                Thread.sleep(2000); // every 2 sec
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    public static void consumeOrders() {
        String url = "jdbc:mysql://localhost:3306/realtime_db";
        String user = "root";
        String password = "1234";

        try {
            Connection con = DriverManager.getConnection(url, user, password);

            String query = "INSERT INTO orders VALUES (?, ?, ?, ?)";
            PreparedStatement ps = con.prepareStatement(query);

            int totalRevenue = 0;

            while (true) {
                String order = queue.take(); // get data

                String[] values = order.split(",");

                int id = Integer.parseInt(values[0]);
                String product = values[1];
                int amount = Integer.parseInt(values[2]);
                String time = values[3];

                
                if (amount <= 0) continue;

                
                ps.setInt(1, id);
                ps.setString(2, product);
                ps.setInt(3, amount);
                ps.setTimestamp(4, Timestamp.valueOf(time));

                ps.executeUpdate();

            
                totalRevenue += amount;

                System.out.println("Consumed: " + order);
                System.out.println("Total Revenue: " + totalRevenue);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
