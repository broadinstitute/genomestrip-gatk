package org.broadinstitute.sting.gatk.filters;

import java.util.*;
import java.util.Map.Entry;
import java.io.File;
import java.io.FileNotFoundException;

import net.sf.picard.filter.SamRecordFilter;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMReadGroupRecord;
import org.broadinstitute.sting.utils.StingException;
import org.broadinstitute.sting.utils.xReadLines;

/**
 * Removes records matching the read group tag and match string.
 * For example each of the filter values:
 *   PU:1000G-mpimg-080821-1_1
 *   PU:1000G-mpimg-080821
 *   PU:mpimg-080821-1_1
 *   PU:mpimg-080821
 *
 * would filter out a read with the read group PU:1000G-mpimg-080821-1_1
 */
public class ReadGroupBlackListFilter implements SamRecordFilter {
    private Set<Entry<String, Collection<String>>> filterEntries;

    public ReadGroupBlackListFilter(List<String> blackLists) {
        Map<String, Collection<String>> filters = new TreeMap<String, Collection<String>>();
        for (String blackList : blackLists)
            addFilter(filters, blackList, null, 0);
        this.filterEntries = filters.entrySet();
    }

    public boolean filterOut(SAMRecord samRecord) {
        for (Entry<String, Collection<String>> filterEntry : filterEntries) {
            String attributeType = filterEntry.getKey();

            Object attribute = samRecord.getAttribute(attributeType);
            if (attribute == null) {
                SAMReadGroupRecord samReadGroupRecord = samRecord.getReadGroup();
                if (samReadGroupRecord != null) {
                    attribute = samReadGroupRecord.getAttribute(attributeType);
                }
            }

            if (attribute != null)
                for (String filterValue : filterEntry.getValue())
                    if (attribute.toString().contains(filterValue))
                        return true;
        }

        return false;
    }

    private void addFilter(Map<String, Collection<String>> filters, String filter, File parentFile, int parentLineNum) {
        if (filter.toLowerCase().endsWith(".list") || filter.toLowerCase().endsWith(".txt")) {
            File file = new File(filter);
            try {
                int lineNum = 0;
                xReadLines lines = new xReadLines(file);
                for (String line : lines) {
                    lineNum++;

                    if (line.trim().length() == 0)
                        continue;

                    if (line.startsWith("#"))
                        continue;

                    addFilter(filters, line, file, lineNum);
                }
            } catch (FileNotFoundException e) {
                String message = "Error loading black list: " + file.getAbsolutePath();
                if (parentFile != null) {
                    message += ", " + parentFile.getAbsolutePath() + ":" + parentLineNum;
                }
                throw new StingException(message, e);
            }
        } else {
            String[] filterEntry = filter.split(":", 2);

            String message = null;
            if (filterEntry.length != 2) {
                message = "Invalid read group filter: " + filter;
            } else if (filterEntry[0].length() != 2) {
                message = "Tag is not two characters: " + filter;
            }

            if (message != null) {
                if (parentFile != null) {
                    message += ", " + parentFile.getAbsolutePath() + ":" + parentLineNum;
                }
                message += ", format is <TAG>:<SUBSTRING>";
                throw new StingException(message);
            }

            if (!filters.containsKey(filterEntry[0]))
                filters.put(filterEntry[0], new TreeSet<String>());
            filters.get(filterEntry[0]).add(filterEntry[1]);
        }
    }
}
