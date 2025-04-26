import java.sql.*;

public class DatabaseUtils {
    public static Connection connect(String dbUrl) throws SQLException {
        return DriverManager.getConnection(dbUrl, Config.DB_USER, Config.DB_PASSWORD);
    }

    public static String fetchLatestSalesData(String dbUrl) {
        StringBuilder data = new StringBuilder();
        String query = "SELECT * FROM sales WHERE updated = 0 ORDER BY id DESC";
        String updateQuery = "UPDATE sales SET updated = 1 WHERE id = ?";
        
        try (Connection conn = connect(dbUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            // Process each row and update 'updates' column to 1
            while (rs.next()) {
                int id = rs.getInt("id");
                data.append(id).append(",")
                    .append(rs.getString("product")).append(",")
                    .append(rs.getInt("quantity")).append(",")
                    .append(rs.getDouble("price")).append("\n");
                
                // Update the 'updates' field for this row
                try (PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {
                    updateStmt.setInt(1, id);
                    updateStmt.executeUpdate();
                }
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return data.toString();
    }        

    public static void insertSalesData(String dbUrl, String data) {
        // Split the incoming data by newline to get each line
        String[] lines = data.split("\n");
        
        for (String line : lines) {
            // Split each line by commas
            String[] parts = line.split(",");
            
            if (parts.length != 4) {
                System.out.println("Invalid data format: " + line);
                continue;  // Skip this line and move to the next
            } 
    
            String id = parts[0];  // The ID
            String product = parts[1];  // The product name (concatenate if necessary)
            int quantity = Integer.parseInt(parts[2]); // Quantity
            double price = Double.parseDouble(parts[3]); // Price
    
            String query = "INSERT INTO sales (id, product, quantity, price) VALUES (?, ?, ?, ?)";
            System.out.println("Query: " + query);
            
            try (Connection conn = connect(dbUrl);
                PreparedStatement stmt = conn.prepareStatement(query)) {
    
                System.out.println("Inserting data: ID=" + id + ", Product=" + product + ", Quantity=" + quantity + ", Price=" + price);
                
                stmt.setInt(1, Integer.parseInt(id));  // Set ID
                stmt.setString(2, product);             // Set product name
                stmt.setInt(3, quantity);               // Set quantity
                stmt.setDouble(4, price);               // Set price
                
                int rowsAffected = stmt.executeUpdate();
                
                if (rowsAffected > 0) {
                    System.out.println("Data inserted successfully.");
                } else {
                    System.out.println("No rows affected.");
                }
    
            } catch (SQLException e) {
                System.out.println("SQL Error: " + e.getMessage());
                e.printStackTrace();
            } catch (NumberFormatException e) {
                System.out.println("Error parsing number: " + e.getMessage());
            }
        }
    }
    
}
