package org.opencompare.stats;

import com.opencsv.CSVReader;

import java.io.*;
import java.util.*;

/**
 * Created by smangin on 7/23/15.
 */
public class Launcher {

    private Grabber grabber = new Grabber();

    public void main(String[] args) throws IOException {
        InputStream inputCSVStream = getClass().getResourceAsStream("/csv/Comparison_of_AMD_processors.csv");
        CSVReader reader = new CSVReader(new InputStreamReader(inputCSVStream), '"', ',');
        for (String[] line: reader.readAll()) {
             Map<Date, String> result = grabber.getVersions("en", "Comparison_(grammar)");
        }

    }
}
