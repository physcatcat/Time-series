package com.ivanovpv.timeseries.tableModel;

import com.ivanovpv.timeseries.interfaces.TrendRepository;

import javax.swing.table.DefaultTableModel;

public class TestTableModel extends DefaultTableModel {
    TrendRepository repository;

    public TestTableModel(TrendRepository repository) {
        this.repository = repository;
        addColumn("id");
        addColumn("Name");
        addColumn("size");
    }

    @Override
    public int getRowCount() {
        if(repository == null) return 0;
        return repository.getRowCountForTest();
    }

    @Override
    public Object getValueAt(int row, int column) {
        if(row < 0 || repository == null) return 0;
        return repository.getValueForTest(row, column);
    }

    public void updateData() {
        fireTableDataChanged();
    }
}
