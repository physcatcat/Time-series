package com.ivanovpv.timeseries.logger;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TSLogger {
    public static void logException(Exception ex) {
        Logger.getLogger(TSLogger.class.getName()).log(Level.SEVERE, "Error!", ex);
    }

    public static void logInfo(String info) {
        Logger.getLogger(TSLogger.class.getName()).log(Level.INFO, info);
    }
}
