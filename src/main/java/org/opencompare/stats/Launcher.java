package org.opencompare.stats;

import scala.util.parsing.json.JSONObject;

import java.io.*;

import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * Created by smangin on 7/23/15.
 */
public class Launcher {

    public static void main(String[] args) throws IOException {
        Launcher launcher = new Launcher();
        InputStream input = launcher.getClass().getResourceAsStream("/list_of_PCMs.csv");
        Grabber grabber = new Grabber("en", "Comparison_of_AMD_processors");
        for (String revid: grabber.getRevIds()) {
            System.out.println(grabber.getDate(revid));
            System.out.println(grabber.getAuthor(revid));
        }

    }
}
