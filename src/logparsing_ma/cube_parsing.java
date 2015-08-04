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
    
    static String       FILE        =   "/Volumes/HD/htdocs/GitHub/log_stuff/log_cube.csv";
    
    static String[]     REGEXS      =   new String[] {  "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[Day Name\\].+([0-9]{8}).+\\[Day Name\\].+([0-9]{8}).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[partner\\].(\\[partner\\].\\[Url\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\{\\[time\\].\\[Day Name\\].+\\[[0-9]{8}\\].+\\[time\\].\\[Day Name\\].+\\[[0-9]{8}\\]\\}.+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+(\\[program\\].\\[name\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\{\\[time\\].\\[Day Name\\].+\\[[0-9]{8}\\].+\\[time\\].\\[Day Name\\].+\\[[0-9]{8}\\]\\}.+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[program\\].\\[program\\].+(\\[Prog Name\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\{\\[time\\].\\[Day Name\\].+\\[([0-9]{8})\\].+\\[time\\].\\[Day Name\\].+\\[([0-9]{8})\\]\\}.+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[program\\].\\[program\\].+(\\[Prog Name\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\{\\[time\\].\\[Day Name\\].+\\[([0-9]{8})\\]\\}.+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[affiliation\\].\\[affiliation_admedia\\].+(\\[admedia key\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+(\\[Prog Name\\]).+\\[(\\d+)\\].+\\{\\[time\\].\\[Day Name\\].+\\[[0-9]{8}\\].+\\[time\\].\\[Day Name\\].+\\[[0-9]{8}\\]\\}.+" //.+\\[partner\\].\\[Url\\].+(\\[\\d+\\]).+"(\\[Prog Name\\]).+\\[(\\d+)\\].+
//                                                     , "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[affiliation\\].\\[affiliation_admedia\\].+(\\[admedia key\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\{\\[time\\].\\[Day Name\\].+\\[(\\d+)\\].+\\[time\\].\\[Day Name\\].+\\[(\\d+)\\]\\}.+" //.+\\[partner\\].\\[Url\\].+(\\[\\d+\\]).+"(\\[Prog Name\\]).+\\[(\\d+)\\].+
                                                    /*,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[Day Name\\].+\\[(\\d+)\\].+\\[Day Name\\].+\\[(\\d+)\\].+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+"};//\",\"(\\d+).+\\[Measures\\].(.+).*"};//.\\[User Name\\].&\\[(.\\d+)\\].*"*/};
// [partner].[partner].[Url].&[2077202]},  {[program].[program].[Prog Name].&[14936]},  {[time].[Day Name].&[20150724]}
//     (  [partner].[User Name].&[1585390],  {[time].[Day Name].&[20150724]},  {} )
    
    static Pattern[]    PATTERNS    = new Pattern[REGEXS.length];
 
    static {
            for (int i = 0; i < REGEXS.length; i++){
                PATTERNS[i] = Pattern.compile(REGEXS[i]);
            }
        }
    
    static void openCube() throws IOException {

        PrintWriter     output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_cube.csv");
        PrintWriter     reg_output =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_cube_NOREGEX.csv");
        
        
        LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(FILE)));
        lnr.skip(Long.MAX_VALUE);
        int countLines      =  lnr.getLineNumber() + 1;
        lnr.close();
        
        try (BufferedReader cube = new BufferedReader(new FileReader(FILE)))
            {
                String sCurrentLine
                    ,   timestamp      
                    ,   range_filter_f 
                    ,   range_filter_t 
                    ,   term_filter    
                    ,   group          
                    ,   sorting  
                    ,   order
                    ,   values         
                    ,   input
                    ;
                int    groupCount
                    ,   i       =   0
                    ,   check   =   0 
                    ,   x       =   0
                    ;
                
                
                output.println("System;TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");
                
                while ((sCurrentLine = cube.readLine()) != null) {
                   
                    i   =   i+1;    //LINE ZÄHLER
                    x   =   0;      //DAMIT DIE NICHT REGEX LINIEN NUR EINMAL GESCHRIEBEN WERDEN
                    
                    System.out.println(i+" von "+countLines);
                    for (Pattern pattern : PATTERNS) {
                       
                        Matcher matcher = pattern.matcher(sCurrentLine);
//                        System.out.println(matcher.matches());
                        if (matcher.matches()){
                            
                            timestamp   =   matcher.group(1);
                            check       =   i;
                            
                            if(matcher.group(2).matches("\\d+")) {  
                                
                                range_filter_f  =   matcher.group(2);//+";"+matcher.group(3);
                                range_filter_t  =   matcher.group(3); 
                                term_filter     =   "PublisherID : "+matcher.group(4);//+matcher.group(5);
                                group           =   "";
                                sorting         =   "";//matcher.group(6);
                                order           =   "";//matcher.group(7);
                                values          =   matcher.group(5);
                            }
                            else{   
                                    
                                if(matcher.group(2).contains("[admedia key]")) { 
                                    
                                    groupCount      =   matcher.groupCount();
                                    
                                    if(groupCount == 8){
                                        
                                        range_filter_f  =   matcher.group(7);
                                        range_filter_t  =   matcher.group(8);
                                        term_filter     =   "PublisherID : "+matcher.group(3)+", "+matcher.group(5)+": "+matcher.group(6);  
                                        group           =   matcher.group(2);
                                        sorting         =   "";//matcher.group(6);
                                        order           =   "";//matcher.group(7);
                                        values          =   matcher.group(4);
                                    }
                                    else{
                                        
                                        range_filter_f  =   matcher.group(5);
                                        range_filter_t  =   matcher.group(6);
                                        term_filter     =   "PublisherID : "+matcher.group(3);
                                        group           =   matcher.group(2);
                                        sorting         =   "";//matcher.group(6);
                                        order           =   "";//matcher.group(7);
                                        values          =   matcher.group(4);
                                    }
                                }
                                else{
                                    //hier muss noch ein group count her... weil manchmal ein timekey im query ist order zwei
                                    groupCount      =   matcher.groupCount();
                                    
                                    if(groupCount == 5){
                                        
                                        range_filter_f  =   matcher.group(5);
                                        range_filter_t  =   "";
                                        term_filter     =   "PublisherID : "+matcher.group(3);
                                        group           =   matcher.group(2);
                                        sorting         =   "";//matcher.group(6);
                                        order           =   "";//matcher.group(7);
                                        values          =   matcher.group(4);
                                    }
                                    else{
                                        range_filter_f  =   "";
                                        range_filter_t  =   "";
                                        term_filter     =   "PublisherID : "+matcher.group(3);
                                        group           =   matcher.group(2);
                                        sorting         =   "";//matcher.group(6);
                                        order           =   "";//matcher.group(7);
                                        values          =   matcher.group(4);
                                    }
                                }
                            }
//                            
                            input   =   "CUBE;"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";"+order+";'"+values+"'";
                            
//                            System.out.println(input);
                            output.println(input); 
                            
                        }
                        else{
                            
                            if(check != i && x == 0){
                                System.out.println(check +"  "+i+" "+x);
                                x   =   1;
                                reg_output.println(sCurrentLine);
                            }
                        }
                    }
                }
            }
        catch (IOException e) 
            {
                e.printStackTrace();
       
            }
                
        output.close();
        System.out.println("Cube done!");
    }
    
}
