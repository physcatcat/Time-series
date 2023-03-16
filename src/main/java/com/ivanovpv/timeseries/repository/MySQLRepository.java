package com.ivanovpv.timeseries.repository;

import com.ivanovpv.timeseries.form.MainApp;
import com.ivanovpv.timeseries.interfaces.TrendRepository;
import com.ivanovpv.timeseries.logger.TSLogger;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MySQLRepository implements TrendRepository {
    private Connection connection;
    private Statement statement;
    private PreparedStatement preparedStatement;
    private List<Integer> curParamIds;
    private List<String> curParamNames;
    private int curTest;
    private int rowCountTrend;
    private static final int BATCH_SIZE = 500;
    private static final boolean CREATE_WITH_DUMP = false;

    private static final boolean USE_WITH_PRELOAD = true;
    private static final int PRELOAD_SIZE = 500;
    private int[] preloadedStartRow;
    private int[] preloadedEndRow;
    private List<Double>[] preloadedValues;

    public MySQLRepository() {
        connectToDB();
        curParamIds = new ArrayList<>();
        curParamNames = new ArrayList<>();
        curTest = -1;
    }

    @Override
    public Object getValueForTest(int row, int column) {
        try {
            preparedStatement = connection.prepareStatement(
                    "select * from `timeseries`.`test` where `idtest` = ?"
            );
            preparedStatement.setInt(1, row + 1);
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            return rs.getObject(column + 1);
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public int getRowCountForTest() {
        int rowCountTest = 0;
        try {
            ResultSet rs = statement.executeQuery(
                    "select count(idtest) from `timeseries`.`test`"
            );
            rs.next();
            rowCountTest = rs.getInt(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return rowCountTest;
    }

    @Override
    public void setCurTest(int curTest) {
        this.curTest = curTest;
        curParamNames.clear();
        curParamIds.clear();
        if (curTest < 0) rowCountTrend = 0;
        try {
            preparedStatement = connection.prepareStatement("select `sizetest` from `timeseries`.`test` where `idtest` = ?");
            preparedStatement.setInt(1, curTest);
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            rowCountTrend = rs.getInt(1);

            preparedStatement = connection.prepareStatement(
                    "select `idparam` from `timeseries`.`param_in_test` where `idtest` = ?"
            );
            preparedStatement.setInt(1, curTest);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                curParamIds.add(rs.getInt(1));
            }
            preparedStatement.close();
            preloadedValues = new List[curParamIds.size()];
            preloadedStartRow = new int[curParamIds.size()];
            preloadedEndRow = new int[curParamIds.size()];
            for (int i = 0; i < curParamIds.size(); i++) {
                preloadedStartRow[i] = Integer.MAX_VALUE;
            }
        } catch (Exception ex) {
            TSLogger.logException(ex);
        }
    }

    @Override
    public List<String> getCurParamNames() {
        try {
            List<String> list = new ArrayList<>();
            for (int i = 0; i < curParamIds.size(); i++) {
                preparedStatement = connection.prepareStatement(
                        "select `name` from `timeseries`.`param` where `idparam` = ?"
                );
                preparedStatement.setInt(1, curParamIds.get(i));
                ResultSet rs = preparedStatement.executeQuery();
                rs.next();
                list.add(rs.getString(1));
            }
            return list;
        } catch (Exception ex) {
            Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    @Override
    public void deleteCurTest() {
        try {
            preparedStatement = connection.prepareStatement(
                    "delete from `timeseries`.`value` where `idtest` = ?"
            );
            preparedStatement.setInt(1, curTest);
            preparedStatement.executeUpdate();

            preparedStatement = connection.prepareStatement(
                    "delete from `timeseries`.`param_in_test` where `idtest` = ?"
            );
            preparedStatement.setInt(1, curTest);
            preparedStatement.executeUpdate();

            preparedStatement = connection.prepareStatement(
                    "delete from `timeseries`.`test` where `idtest` = ?"
            );
            preparedStatement.setInt(1, curTest);
            preparedStatement.executeUpdate();
        } catch (Exception ex) {
            Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
        }
        setCurTest(-1);
    }

    @Override
    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public Object getValueForTrend(int row, int column) {
        double val = 0;
        if(USE_WITH_PRELOAD) {
            preload(row, column);
            return preloadedValues[column].get(row-preloadedStartRow[column]);
        }
        try {
            preparedStatement = connection.prepareStatement(
                    "select `value` from `timeseries`.`value` where `idtest` = ? and `num` = ? and `idparam` = ?"
            );
            preparedStatement.setInt(1, curTest);
            preparedStatement.setInt(2, row);
            preparedStatement.setInt(3, curParamIds.get(column));
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            val = rs.getDouble(1);
            preparedStatement.close();
        } catch (SQLException ex) {
            Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
        }
        return val;
    }

    private void preload(int row, int column) {
        try {
            if (row < preloadedStartRow[column] || row > preloadedEndRow[column]) {
                preloadedStartRow[column] = row - PRELOAD_SIZE;
                if (preloadedStartRow[column] < 0) preloadedStartRow[column] = 0;
                preloadedEndRow[column] = row + PRELOAD_SIZE;
                preparedStatement = connection.prepareStatement(
                        "SELECT `value` from `timeseries`.`value` where `idtest` = ? and `num` >= ? and `num` <= ? and `idparam` = ? order by `num`"
                );
                preparedStatement.setInt(1, curTest);
                preparedStatement.setInt(2, preloadedStartRow[column]);
                preparedStatement.setInt(3, preloadedEndRow[column]);
                preparedStatement.setInt(4, curParamIds.get(column));
                ResultSet rs = preparedStatement.executeQuery();
                preloadedValues[column] = new ArrayList<>();
                while (rs.next()) {
                    preloadedValues[column].add(rs.getDouble(1));
                }
            }
        } catch (Exception ex) {
            TSLogger.logException(ex);
        }
    }

    @Override
    public int getRowCountForTrend() {
        return rowCountTrend;
    }

    @Override
    public void importToDB(String fileName) {
        try (FileReader reader = new FileReader(fileName)) {
            Scanner scanner = new Scanner(reader);
            int testId = getNewTestId();
            int[] paramsId;

            //Добавляем новое испытание
            preparedStatement = connection.prepareStatement(
                    "INSERT INTO `timeseries`.`test` (`idtest`, `nametest`) VALUES (?, ?)"
            );
            preparedStatement.setInt(1, testId);
            preparedStatement.setString(2, getTestName(fileName));
            preparedStatement.executeUpdate();
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Created a test");

            //Добавляем параметры
            String[] names = scanner.nextLine().split("\t");
            String[] units = scanner.nextLine().split("\t");

            for (int i = 0; i < names.length; i++) {
                if (!isParamExist(names[i], units[i])) {
                    preparedStatement = connection.prepareStatement(
                            "INSERT INTO `timeseries`.`param` (`idparam`, `name`, `units`) VALUES (?, ?, ?)"
                    );
                    preparedStatement.setInt(1, getNewParamId());
                    preparedStatement.setString(2, names[i]);
                    preparedStatement.setString(3, units[i]);
                    preparedStatement.executeUpdate();
                }
            }
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Created parameters");

            //Добавляем параметры в испытаниях
            paramsId = getParamIds(names, units);
            for (int i = 0; i < names.length; i++) {
                preparedStatement = connection.prepareStatement(
                        "INSERT INTO `timeseries`.`param_in_test` (`idtest`, `idparam`) VALUES (?, ?)"
                );
                preparedStatement.setInt(1, testId);
                preparedStatement.setInt(2, paramsId[i]);
                preparedStatement.executeUpdate();
            }
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Created params in test");

            //Добавляем значения
            if (CREATE_WITH_DUMP) {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Using create with dump");
                int num = 0;
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName + "_dump.txt"));
                while (scanner.hasNextLine()) {
                    Double[] values = Arrays.stream(scanner.nextLine().split("\t"))
                            .map(v -> v = v.replace(",", "."))
                            .map(Double::parseDouble)
                            .toArray(Double[]::new);
                    for (int i = 0; i < values.length; i++) {
                        bufferedWriter.write(num + " " + values[i] + " " + testId + " " + paramsId[i] + "\n");
                        bufferedWriter.flush();
                    }
                    num++;
                    if (num % 5000 == 0) {
                        Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Wrote " + num + " lines in SQL-dump file");
                    }
                }
                bufferedWriter.close();

                preparedStatement = connection.prepareStatement("" +
                        "UPDATE `timeseries`.`test` SET `sizetest` = ? WHERE (`idtest` = ?)"
                );
                preparedStatement.setInt(1, num);
                preparedStatement.setInt(2, testId);
                preparedStatement.executeUpdate();
            } else {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Using direct writing in MySQL");
                int num = 0;
                //создаём шаблон запроса (множественная вставка по n записей)
                StringBuilder sqlQ = new StringBuilder("INSERT INTO `timeseries`.`value` (`num`, `value`, `idtest`, `idparam`) VALUES (?, ?, ?, ?)");
                for (int i = 1; i < paramsId.length; i++) {
                    sqlQ.append(", (?, ?, ?, ?)");
                }
                preparedStatement = connection.prepareStatement(sqlQ.toString());
                while (scanner.hasNextLine()) {
                    Double[] values = Arrays.stream(scanner.nextLine().split("\t"))
                            .map(v -> v = v.replace(",", "."))
                            .map(Double::parseDouble)
                            .toArray(Double[]::new);
                    for (int i = 0; i < values.length; i++) {
                        preparedStatement.setInt(1 + 4 * i, num);
                        preparedStatement.setDouble(2 + 4 * i, values[i]);
                        preparedStatement.setInt(3 + 4 * i, testId);
                        preparedStatement.setInt(4 + 4 * i, paramsId[i]);
                    }
                    preparedStatement.addBatch();
                    num++;

                    if ((num % BATCH_SIZE == 0 || !scanner.hasNextLine()) && num != 0) {
                        long ms = System.nanoTime();
                        preparedStatement.executeBatch();
                        Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Executed " + BATCH_SIZE +
                                " batches in value table in " + (System.nanoTime() - ms) / 1_000_000 + " ms. " + num + " lines was written");
                    }
                }
                preparedStatement = connection.prepareStatement("" +
                        "UPDATE `timeseries`.`test` SET `sizetest` = ? WHERE (`idtest` = ?)"
                );
                preparedStatement.setInt(1, num);
                preparedStatement.setInt(2, testId);
                preparedStatement.executeUpdate();
            }
            preparedStatement.close();
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Import was done");
        } catch (Exception ex) {
            Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void connectToDB() {
        try {
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Connecting to MySQL...");
            Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
            connection = DriverManager.getConnection("jdbc:mysql://localhost/timeseries", "root", "root");
            statement = connection.createStatement();
            statement.execute("use timeseries");
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Connected successful");
        } catch (Exception ex) {
            Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private String getTestName(String fileName) {
        return fileName.substring(fileName.lastIndexOf('\\') + 1, fileName.lastIndexOf('.'));
    }

    private int getNewTestId() throws SQLException {
        ResultSet rs = statement.executeQuery("select max(`idtest`) from `timeseries`.`test`");
        rs.next();
        return rs.getInt(1) + 1;
    }

    private int getNewParamId() throws SQLException {
        ResultSet rs = statement.executeQuery("select max(`idparam`) from `timeseries`.`param`");
        rs.next();
        return rs.getInt(1) + 1;
    }

    private int[] getParamIds(String[] names, String[] units) throws SQLException {
        int[] res = new int[names.length];
        for (int i = 0; i < res.length; i++) {
            preparedStatement = connection.prepareStatement("select `idparam` from `timeseries`.`param` where `name` = ? and `units` = ?");
            preparedStatement.setString(1, names[i]);
            preparedStatement.setString(2, units[i]);
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            res[i] = rs.getInt(1);
        }
        return res;
    }

    private boolean isParamExist(String name, String units) throws SQLException {
        preparedStatement = connection.prepareStatement(
                "select * from `timeseries`.`param` where `name` = ? and `units` = ?"
        );
        preparedStatement.setString(1, name);
        preparedStatement.setString(2, units);
        ResultSet rs = preparedStatement.executeQuery();
        return rs.next();
    }

}
