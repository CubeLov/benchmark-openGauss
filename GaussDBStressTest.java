import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GaussDBStressTest {

    private static final String DB_URL = "jdbc:postgresql://localhost:15432/project3";
    private static final String USER = "gaussdb";
    private static final String PASSWORD = "DkhZyd@520";
    private static final int THREAD_COUNT = 200;  // Number of concurrent threads
    private static final int OPERATION_COUNT = 100;  // Operations per thread

    // 创建一个 AtomicInteger 来保证线程安全的 id 自增
    private static final AtomicInteger idGenerator = new AtomicInteger(1);

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.execute(new DatabaseTask());
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Total time: " + (endTime - startTime) + " ms");
    }

    static class DatabaseTask implements Runnable {

        @Override
        public void run() {
            try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
                for (int i = 0; i < OPERATION_COUNT; i++) {
                    int id = idGenerator.getAndIncrement();  // 获取并自增 id
                    executeOperation(connection, id);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void executeOperation(Connection connection, int id) throws SQLException {
            // 使用 id 插入数据
            String sql = "INSERT INTO test_table (id, data) VALUES (?,?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setInt(1, id);
                preparedStatement.setString(2, "Sample data " + id);
                preparedStatement.executeUpdate();
            }
        }
    }
}
