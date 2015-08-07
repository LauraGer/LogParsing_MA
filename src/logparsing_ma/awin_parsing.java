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
import static logparsing_ma.cube_parsing.FILE;
/**
 *
 * @author Laura
 */
class awin_parsing {
    
         //(USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)
    static String       FILE        =  "/Volumes/HD/X/LOGS/awin_edit.csv";
    
    static String[]     REGEXS      = new String[] {    "\".+,\"(\\d{4}-\\d{2}-\\d{2}\\d{2}:\\d{2}:\\d{2})\",\"(\\d{4}-\\d{2}-\\d{2})\",\"(\\d{4}-\\d{2}-\\d{2})\",\"(.+)([DESC|ASC])\",\"[a-z]+\\.(\\d+)\",.+\"(SELECT.+)FROM.+\""
                                                    ,   "\".+,\"(\\d{4}-\\d{2}-\\d{2}\\d{2}:\\d{2}:\\d{2})\",\"(\\d{4}-\\d{2}-\\d{2})\",\"(\\d{4}-\\d{2}-\\d{2})\",\"(.+)([DESC|ASC])\",\"([a-z]+\\.[a-z]+)\",.+\"(SELECT.+)FROM.+(\\d+)= mem.affiliate_id.+\""};//\",\"(.+)([DESC|ASC]).+"};//\",\"[.+.(\\d)|([a-z].[a-z])]\",\"(.+)\",\".+,\"(.+)FROM.*"};
    
    static Pattern[]    PATTERNS    = new Pattern[REGEXS.length];
    
    static {
        for (int i = 0; i < REGEXS.length; i++){
            PATTERNS[i] = Pattern.compile(REGEXS[i]);
        }
    }
    
    public static void openAwin() throws IOException {
        
        String  timestamp      
            ,   range_filter_f 
            ,   range_filter_t 
            ,   term_filter    
            ,   group 
            ,   order
            ,   sorting        
            ,   values         
            ,   input
            ;
        
        int i = 0;
        
        LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(FILE)));
        lnr.skip(Long.MAX_VALUE);
        int countLines      =  lnr.getLineNumber() + 1;
        lnr.close();
        
        PrintWriter  output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_awin.csv");
           
        String  replacedTxt     =   logparsing_methods.readFile("/Volumes/HD/htdocs/GitHub/log_stuff/log_awin.txt")

                                            .replaceAll(" ", "")
                                                .replaceAll("[\\n\\r]", "")
                                                    .replaceAll("\"", "\"")
                                                        .replaceAll("',", "\",\"")
                                                            .replaceAll(",'", "\",\"")
                                                                .replaceAll("54\\d{5}|[0-9]{7}", "\r\"$0\"");

        logparsing_methods.writeFile("/Volumes/HD/X/LOGS/awin_edit.csv", replacedTxt);
                
        output.println("\"System\";TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");
            
           
        try (BufferedReader awin = new BufferedReader(new FileReader(FILE)))
        {
            String sCurrentLine;

            while ((sCurrentLine = awin.readLine()) != null) {
                 
                i   =   i+1;    //LINE ZÄHLER
                
                
                System.out.println(i+" von "+countLines);
                
                for (Pattern pattern : PATTERNS) {

                    Matcher matcher = pattern.matcher(sCurrentLine);

                    if (matcher.matches()){

                        timestamp       =   matcher.group(1);
                        range_filter_f  =   matcher.group(2);
                        range_filter_t  =   matcher.group(3);
                        term_filter     =   matcher.group(6);
                        group           =   matcher.group(4);
                        sorting         =   matcher.group(5);
                        order           =   "";
                        values          =   matcher.group(7);

                        input       =   ("'AWIN';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";"+order+";"+values+"'");

                        output.println(input);
                    }  
                }    

            }

        } 
        catch (IOException e) {
        } 
        output.close();
        System.out.println("AWIN done!");
    }
}
