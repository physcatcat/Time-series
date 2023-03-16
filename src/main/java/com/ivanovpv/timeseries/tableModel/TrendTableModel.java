package com.ivanovpv.timeseries.tableModel;

import com.ivanovpv.timeseries.interfaces.TrendRepository;

import javax.swing.table.DefaultTableModel;
import java.util.List;


/**
 * @author Pavel
 */
public class TrendTableModel extends DefaultTableModel {
    TrendRepository repository;
    public TrendTableModel(TrendRepository repository) {
        this.repository = repository;
    }
    @Override
    public int getRowCount() {
        if(repository == null) return 0;
        return repository.getRowCountForTrend();
    }

    @Override
    public Object getValueAt(int row, int column) {
        return repository.getValueForTrend(row, column);
    }

    public void updateData() {
        setColumnCount(0);
        List<String> cols = repository.getCurParamNames();
        for (int i = 0; i < cols.size(); i++) {
            addColumn(cols.get(i));
        }
    }
}
