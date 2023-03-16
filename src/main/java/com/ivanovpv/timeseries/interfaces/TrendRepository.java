package com.ivanovpv.timeseries.interfaces;

import java.util.List;

public interface TrendRepository {
    void importToDB(String fileName);

    Object getValueForTest(int row, int column);

    int getRowCountForTest();

    Object getValueForTrend(int row, int column);

    int getRowCountForTrend();

    void setCurTest(int testId);

    List<String> getCurParamNames();

    void deleteCurTest();

    void closeConnection();
}
