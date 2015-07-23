package org.opencompare.stats;

import org.opencompare.io.wikipedia.io.MediaWikiAPI;

import java.util.*;

/**
 * Created by smangin on 7/23/15.
 *
 * Use to get all revision from a single wikipedia page
 *
 */
public class Grabber {

    private MediaWikiAPI api;

    public Grabber() {
        api = new MediaWikiAPI("https", "wikipedia.com");
    }

    public Map<Date, String> getVersions(String lang, String title) {
        String result = api.getVersionFromTitle(lang, title);
        System.out.println(result);
        return new HashMap<Date, String>();
    }
}
