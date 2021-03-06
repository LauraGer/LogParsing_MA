/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static logparsing_ma.Regex.PATTERNS;
import static logparsing_ma.cube_parsing.FILE;

/**
 *
 * @author Laura
 */
class hadoop_parsing {

    static String SEP = File.separator;
    //(USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)
//    static String FILE = "C:"+SEP+"LOGS"+SEP+"FILES"+SEP+"Hadoop_Logs"+SEP+"merge-BOOKING_SEARCH-0
    static String FILE = SEP + "Volumes" + SEP + "HD" + SEP + "htdocs" + SEP + "GitHub" + SEP + "log_stuff" + SEP + "log_hadoop";

    static String[] REGEXS = new String[]{"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\(Filters \\[\\[(.+)\\]\\]\\).+\\(GroupFields \\[(.+)\\]\\)\\(ValueFields \\[(.+)\\]\\)\\(DistinctFields.+"};
    /*   "(.+) INFO .+ (USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)\\)\\(Version 7\\)\\(Values \\[(\\d+)\\].+TIME_STAMP\\)\\(Version 7\\)\\(LowerTerm (\\d+)\\)\\(IncludeLower true\\)\\(UpperTerm (\\d+).*\\(GroupFields \\[(.+)\\]\\).*\\(ValueFields \\[(.+).*\\(Sort \\[WSort \\(Version 7\\)\\((.+)([ DESC| ASC])\\)\\]\\)\\(.*"
     ,   "(.+) INFO .+ (USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)\\)\\(Version 7\\)\\(Values \\[(\\d+)\\].+TIME_STAMP\\)\\(Version 7\\)\\(LowerTerm (\\d+)\\)\\(IncludeLower true\\)\\(UpperTerm (\\d+).*\\(GroupFields \\[(.+)\\]\\).*\\(ValueFields \\[(.+).*\\(Sort \\[WSort \\(Version 7\\)\\]\\)\\(.*"};*/
    static Pattern[] PATTERNS = new Pattern[REGEXS.length];

    static {
        for (int i = 0; i < REGEXS.length; i++) {
            PATTERNS[i] = Pattern.compile(REGEXS[i]);
        }
    }

    public static void openHadoop() throws IOException {

        String timestamp;
        String range_filter_f;
        String range_filter_t;
        String term_filter;
        String group;
        String order;
        String sorting;
        String values;
        String input;

        int i = 0;
        int count = 0;

        LineNumberReader lnr = new LineNumberReader(new FileReader(new File(FILE)));
        lnr.skip(Long.MAX_VALUE);
        int countLines = lnr.getLineNumber() + 1;
        lnr.close();

        PrintWriter output = new PrintWriter(SEP + "Volumes" + SEP + "HD" + SEP + "X" + SEP + "LOGS" + SEP + "FINAL_hadoop.csv");
        PrintWriter reg_output = new PrintWriter(SEP + "Volumes" + SEP + "HD" + SEP + "X" + SEP + "LOGS" + SEP + "FINAL_hadoop_NOREGEX.csv");

        try (BufferedReader hadoop = new BufferedReader(new FileReader(FILE))) {
            String sCurrentLine;

            output.println("System;TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");

            while ((sCurrentLine = hadoop.readLine()) != null) {

                i = i + 1;    //LINE ZÄHLER

                System.out.println(i + " von " + countLines);

                if ((sCurrentLine.contains("WQuery"))) {

                    for (Pattern pattern : PATTERNS) {

                        Matcher matcher = pattern.matcher(sCurrentLine);

                        if (matcher.matches()) {

                            // Wenn pattern nicht gefunden, dann sysoutprtln (um neues regex zu bauen)
                            timestamp = matcher.group(1);
                            range_filter_f = matcher.group(2).replaceAll(".+\\(Field TIME_STAMP\\)\\(Version 7\\)\\(LowerTerm (\\d{8})\\).+", "$1");
                            range_filter_t = matcher.group(2).replaceAll(".+\\(Field TIME_STAMP\\)\\(Version 7\\).+\\(UpperTerm (\\d{8})\\).+", "$1");

                            Pattern find_pattern = Pattern.compile("\\bWTermFilter\\b");

                            Matcher find_matcher = find_pattern.matcher(matcher.group(2));

                            count = 0;

                            while (find_matcher.find()) {
                                count++;
                            }

                            if (count == 2) {
                                term_filter = matcher.group(2).replaceAll(".*WTermFilter \\(Field (.+)\\)\\(Version 7\\)\\(Values \\[(.+)\\]\\)\\],.+WTermFilter \\(Field (.+)\\)\\(Version 7\\)\\(Values \\[(.+)\\]\\).*", "$1 : $2 - $3 : $4");
                            } else if (count == 3) {
                                term_filter = matcher.group(2).replaceAll(".*WTermFilter \\(Field (.+)\\)\\(Version 7\\)\\(Values \\[(.+)\\]\\)\\],.+WTermFilter \\(Field (.+)\\)\\(Version 7\\)\\(Values \\[(.+)\\]\\)\\],.+WTermFilter \\(Field (.+)\\)\\(Version 7\\)\\(Values \\[(.+)\\]\\).*", "$1 : $2 - $3 : $4 - $5 : $6");
                            } else {
                                term_filter = matcher.group(2).replaceAll(".*WTermFilter \\(Field (.+)\\)\\(Version 7\\)\\(Values \\[(.+)\\]\\)(,)*.*", "$1 : $2");
                            }
                            term_filter = term_filter.replace(":","");
                            group = matcher.group(3);
                            sorting = "";//matcher.group(8);
                            order = "";//matcher.group(9);
                            values = matcher.group(4);

                            input = "HADOOP;" + timestamp + ";" + range_filter_f + ";" + range_filter_t + ";" + term_filter + ";" + group + ";" + sorting + ";" + order + ";" + values + "";

                            output.println(input);
                        } else {
                            reg_output.println(sCurrentLine);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();

        }

        output.close();
        reg_output.close();
        System.out.println("Hadoop done!");
    }
}
