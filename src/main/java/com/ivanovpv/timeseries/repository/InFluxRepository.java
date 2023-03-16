package com.ivanovpv.timeseries.repository;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;
import com.ivanovpv.timeseries.interfaces.TrendRepository;
import com.ivanovpv.timeseries.logger.TSLogger;

import java.io.FileReader;
import java.time.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class InFluxRepository implements TrendRepository {
    private InfluxDBClient client;
    private final String token = "0-_KYebdFhysr8rQwWHhGZlZSbPspNkpUOIP4UGv0_PYyCz_btHa_iisNcHWAyxBEL192gtlQPYWLDK31ffIaQ==";
    private final String bucket = "timeseries";
    private final String org = "root";
    private WriteApiBlocking writeApi;
    private QueryApi queryApi;
    private List<String> testNames;
    private List<Integer> testSizes;
    private List<String> curParamNames;
    private int curTestId;
    private static final int BATCH_SIZE = 5000;
    private static final boolean USE_WITH_PRELOAD = true;
    private static final int PRELOAD_SIZE = 250;
    private int[] preloadedStartRow;
    private int[] preloadedEndRow;
    private FluxTable[] preloadedValues;

    public InFluxRepository() {
        connectToDB();
        initTests();
        curTestId = -1;
    }

    @Override
    public void importToDB(String fileName) {
        TSLogger.logInfo("Starting import");
        try (FileReader fileReader = new FileReader(fileName); Scanner scanner = new Scanner(fileReader)) {
            //Добавляем новое испытание
            String testName = getTestName(fileName);

            //Считываем параметры и их ед. изм.
            String[] names = scanner.nextLine().split("\t");
            String[] units = scanner.nextLine().split("\t");

            //Добавляем параметры
            List<String> points = new ArrayList<>();

            int num = 0;
            long ms = System.currentTimeMillis();

            while (scanner.hasNextLine()) {
                Double[] values = Arrays.stream(scanner.nextLine().split("\t"))
                        .map(v -> v = v.replace(",", "."))
                        .map(Double::parseDouble)
                        .toArray(Double[]::new);

                for (int i = 0; i < values.length; i++) {
                    points.add(Point
                            .measurement(testName)
                            .addField(names[i], values[i])
                            .addTag("Units", units[i])
                            .time(num, WritePrecision.S)
                            .toLineProtocol() + "\n"
                    );
                }

                num++;
                if (num % (BATCH_SIZE) == 0) {
                    writeApi.writeRecords(bucket, org, WritePrecision.S, points);
                    points = new ArrayList<>();
                    TSLogger.logInfo("Executed " + BATCH_SIZE + " lines in " + (System.currentTimeMillis() - ms) + " ms. Wrote " + num + " lines");
                    ms = System.currentTimeMillis();
                }
            }
            writeApi.writePoint(
                    bucket,
                    org,
                    Point.measurement(testName)
                            .addField("size", num)
            );
        } catch (Exception ex) {
            TSLogger.logException(ex);
        }
    }

    @Override
    public Object getValueForTest(int row, int column) {
        switch (column) {
            case 0:
                return row;
            case 1:
                return testNames.get(row);
            case 2:
                return testSizes.get(row);
        }
        return null;
    }

    @Override
    public int getRowCountForTest() {
        return testNames.size();
    }

    @Override
    public Object getValueForTrend(int row, int column) {
        //TODO: подумать над заобором данных из БД
        if (USE_WITH_PRELOAD) {
            preload(row, column);
            return preloadedValues[column].getRecords().get(row - preloadedStartRow[column]).getValue();
        } else {
            String q = "from(bucket: \"timeseries\")" +
                    "|> range(start:" + row + ", stop:" + (row + 1) + ")" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"" + testNames.get(curTestId) + "\")" +
                    "|> filter(fn: (r) => r[\"_field\"] == \"" + curParamNames.get(column) + "\")";
            return queryApi.query(q, org).get(0).getRecords().get(0).getValue();
        }
    }

    @Override
    public int getRowCountForTrend() {
        if (curTestId < 0) return 0;
        return testSizes.get(curTestId);
    }

    @Override
    public void setCurTest(int testId) {
        curTestId = testId;
        if (curTestId < 0) {
            curParamNames = new ArrayList<>();
            return;
        }

        List<FluxTable> list = queryApi.query(
                "import \"influxdata/influxdb/schema\"" +
                        "schema.measurementFieldKeys(" +
                        "bucket: \"timeseries\"," +
                        "measurement: \"" + testNames.get(curTestId) + "\"," +
                        "start: 0" +
                        ")",
                org);

        curParamNames = list.get(0).getRecords().stream().map(r -> r.getValue().toString()).collect(Collectors.toList());
        curParamNames.remove("size");
        preloadedValues = new FluxTable[curParamNames.size()];
        preloadedStartRow = new int[curParamNames.size()];
        preloadedEndRow = new int[curParamNames.size()];
        for (int i = 0; i < curParamNames.size(); i++) {
            preloadedStartRow[i] = Integer.MAX_VALUE;
        }
    }

    @Override
    public List<String> getCurParamNames() {
        return curParamNames;
    }

    @Override
    public void deleteCurTest() {
        OffsetDateTime min = OffsetDateTime.of(LocalDate.of(1968, 1, 1), LocalTime.of(0, 0), ZoneOffset.MIN);
        OffsetDateTime max = OffsetDateTime.now();
        client.getDeleteApi().delete(min, max, "_measurement=\"" + testNames.get(curTestId) + "\"",
                client.getBucketsApi().findBucketByName("timeseries"),
                client.getOrganizationsApi().findOrganizationByID("c40e81697b1aee82")
        );
    }

    @Override
    public void closeConnection() {
        try {
            client.close();
        } catch (Exception ex) {
            TSLogger.logException(ex);
        }
    }

    private void connectToDB() {
        try {
            TSLogger.logInfo("Connecting to InfluxDB...");
            client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());
            writeApi = client.getWriteApiBlocking();
            queryApi = client.getQueryApi();
            TSLogger.logInfo("Connected successful");
        } catch (Exception ex) {
            TSLogger.logException(ex);
        }
    }

    private String getTestName(String fileName) {
        return fileName.substring(fileName.lastIndexOf('\\') + 1, fileName.lastIndexOf('.'));
    }

    private void initTests() {
        List<FluxTable> tests = queryApi.query(
                "import \"influxdata/influxdb/schema\"" +
                        "schema.measurements(" +
                        "bucket: \"timeseries\"," +
                        ")",
                org
        );
        testNames = tests.get(0).getRecords().stream().map(r -> r.getValue().toString()).collect(Collectors.toList());

        testSizes = new ArrayList<>();
        testNames.forEach(test -> {
            List<FluxTable> size = queryApi.query(
                    "from(bucket: \"timeseries\")" +
                            "|> range(start: 0)" +
                            "|> filter(fn: (r) => r[\"_measurement\"] == \"" + test + "\")" +
                            "|> filter(fn: (r) => r[\"_field\"] == \"size\")",
                    org
            );
            testSizes.add(Integer.parseInt(size.get(0).getRecords().get(0).getValue().toString()));
        });
    }

    private void preload(int row, int column) {
        if (row < preloadedStartRow[column] || row > preloadedEndRow[column]) {
            preloadedStartRow[column] = row - PRELOAD_SIZE;
            if (preloadedStartRow[column] < 0) preloadedStartRow[column] = 0;
            preloadedEndRow[column] = row + PRELOAD_SIZE;
            String q = "from(bucket: \"timeseries\")" +
                    "|> range(start:" + preloadedStartRow[column] + ", stop:" + (preloadedEndRow[column] + 1) + ")" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"" + testNames.get(curTestId) + "\")" +
                    "|> filter(fn: (r) => r[\"_field\"] == \"" + curParamNames.get(column) + "\")";
            preloadedValues[column] = queryApi.query(q, org).get(0);
        }
    }
}
