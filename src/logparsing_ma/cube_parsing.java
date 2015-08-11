/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static logparsing_ma.hadoop_parsing.PATTERNS;

/**
 *
 * @author Laura
 */
class cube_parsing {

    static String SEP = File.separator;
//    static String FILE = "C:"+SEP+"LOGS"+SEP+"FILES"+SEP+"Cube_ElasticSearchLogs"+SEP+"cube_log
    static String FILE = SEP + "Volumes" + SEP + "HD" + SEP + "htdocs" + SEP + "GitHub" + SEP + "log_stuff" + SEP + "log_cube.csv";

    static String[] REGEXS = new String[]{"\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\",+\"(.+)\",+\"L-JBOSS-.+SELECT \\{(.+)\\} ON COLUMNS.+EMPTY.+\\{(.+)\\} \\} ON ROWS.+WHERE \\( (.+) \\) \",\".+\""};
//     "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\".+\"(.+)\".+\"(\\d+)\".+\"L-JBOSS-.+SELECT \\{(.+)\\} ON COLUMNS.+WHERE \\( (.+) \\) \",\".+\""};
//                                         ,  "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\".+\"(.+)\".+\"L-JBOSS-.+SELECT \\{(.+)\\} ON COLUMNS.+WHERE \\( (.+) \\) \",\".+\"
//.+\".+\"\\d+
    static Pattern[] PATTERNS = new Pattern[REGEXS.length];

    static {
        for (int i = 0; i < REGEXS.length; i++) {
            PATTERNS[i] = Pattern.compile(REGEXS[i]);
        }
    }

    static void openCube() throws IOException {

        PrintWriter output = new PrintWriter(SEP + "Volumes" + SEP + "HD" + SEP + "X" + SEP + "LOGS" + SEP + "FINAL_cube.csv");
        PrintWriter reg_output = new PrintWriter(SEP + "Volumes" + SEP + "HD" + SEP + "X" + SEP + "LOGS" + SEP + "FINAL_cube_NOREGEX.csv");

        LineNumberReader lnr = new LineNumberReader(new FileReader(new File(FILE)));
        lnr.skip(Long.MAX_VALUE);
        int countLines = lnr.getLineNumber() + 1;
        lnr.close();

        try (BufferedReader cube = new BufferedReader(new FileReader(FILE))) {
            String sCurrentLine, timestamp, range_filter, range_filter_t, term_filter, group, sorting, order, values, input;
            int i = 0;

            output.println("System;TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");

            while ((sCurrentLine = cube.readLine()) != null) {

                i = i + 1;    //LINE ZÄHLER

                System.out.println(i + " von " + countLines);

                for (Pattern pattern : PATTERNS) {

                    Matcher matcher = pattern.matcher(sCurrentLine);
//                        System.out.println(matcher.matches());
                    if (matcher.matches()) {
                        timestamp = matcher.group(1);
                        range_filter = matcher.group(2);
                        if (matcher.group(2).matches(".*\\[time\\]\\.\\[Day Name\\]\\.&\\[\\d+\\] : \\[time\\]\\.\\[Day Name\\]\\.&\\[\\d+\\].*")) {
                            range_filter = matcher.group(2)
                                    .replaceAll(".*\\[time\\]\\.\\[Day Name\\]\\.&\\[(\\d+)\\] : \\[time\\]\\.\\[Day Name\\]\\.&\\[(\\d+)\\].*", "$1 ; $2");//+";"+matcher.group(3)
                        } else if (matcher.group(2).matches(".*\\[time\\]\\.\\[Month Name\\]\\.&\\[\\d+\\] : \\[time\\]\\.\\[Month Name\\]\\.&\\[\\d+\\].*")) {
                            range_filter = matcher
                                    .group(2).replaceAll(".*\\[time\\]\\.\\[Month Name\\]\\.&\\[(\\d+)\\] : \\[time\\]\\.\\[Month Name\\]\\.&\\[(\\d+)\\].*", "$1 ; $2");//+";"+matcher.group(3);
                        } else if (matcher.group(5).matches(".*\\{\\[time\\]\\.\\[Day Name\\]\\.&\\[\\d+\\]\\}.*")) {
                            range_filter = matcher
                                    .group(5).replaceAll(".*\\{\\[time\\]\\.\\[Day Name\\]\\.&\\[(\\d+)\\]\\}", "$1 ;");
                        } else if (matcher.group(5).matches(".*\\{\\[time\\]\\.\\[Month Name\\]\\.&\\[\\d+\\]\\}.*")) {
                            range_filter = matcher
                                    .group(5).replaceAll(".*\\{\\[time\\]\\.\\[Month Name\\]\\.&\\[(\\d+)\\]\\}", "$1 ;");
                        } else if (matcher.group(5).matches(".*\\{\\[time\\]\\.\\[Day Name\\]\\.&\\[\\d+\\] : \\[time\\]\\.\\[Day Name\\]\\.&\\[\\d+\\]\\}.*")) {
                            range_filter = matcher
                                    .group(5).replaceAll(".*\\{\\[time\\]\\.\\[Day Name\\]\\.&\\[(\\d+)\\] : \\[time\\]\\.\\[Day Name\\]\\.&\\[(\\d+)\\]\\}.*", "$1 ; $2 ");//+";"+matcher.group(3);
                        } else if (matcher.group(5).matches(".*\\{\\[time\\]\\.\\[Month Name\\]\\.&\\[\\d+\\] : \\[time\\]\\.\\[Month Name\\]\\.&\\[\\d+\\]\\}.*")) {
                            range_filter = matcher
                                    .group(5).replaceAll(".*\\{\\[time\\]\\.\\[Month Name\\]\\.&\\[(\\d+)\\] : \\[time\\]\\.\\[Month Name\\]\\.&\\[(\\d+)\\]\\}.*", "$1 ; $2");//+";"+matcher.group(3);
                        } else {
                            range_filter = ";";
                        }

                        range_filter = range_filter
                                .replace(",  {}", "");

//                        if (matcher.group(2).matches(".*\"(\\d+)")) {
//                            term_filter = "publisher_id " + matcher.group(2)
//                                    .replaceAll(".*\"(\\d+)", "$1");//matcher.group(3);
//                            term_filter = term_filter + " - ";
//                        } else {
//                            term_filter = "";
//                        }

                        term_filter = matcher.group(5)
                                .replace("}", "")
                                .replace("{", "")
                                .replace("[", "")
                                .replace("]", "")
                                .replace("&", "")
                                .replace(",", " -");
                        group = matcher.group(4)
                                .replace("}", "")
                                .replace("{", "")
                                .replace("[", "")
                                .replace("]", "")
                                .replace("&", "");
                        sorting = "";
                        order = "";
                        values = matcher.group(3)
                                .replace("[Measures].", "")
                                .replace("[", "")
                                .replace("]", "")
                                .replace("&", "")
                                .replace("'","");
                        input = "CUBE;" + timestamp + ";" + range_filter + ""/* + range_filter_t */ + ";" + term_filter + ";" + group + ";" + sorting + ";" + order + ";'" + values + "'";
                        output.println(input);
//                            
                    } else {

                        reg_output.println(sCurrentLine);
//                          
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();

        }

        output.close();
        reg_output.close();
        System.out.println("Cube done!");
    }

}
