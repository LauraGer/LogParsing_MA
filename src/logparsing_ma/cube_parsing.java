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
    
    static String[]     REGEXS      = new String[] {    /*"\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[time\\].\\[Day Name\\].+(\\d{5}|[0-9]{10}).+\\[time\\].\\[Day Name\\].+(\\d+).+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[partner\\].(\\[partner\\].\\[Url\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\[Day Name\\].+\\[(\\d+)\\].+"
                                                    ,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+(\\[program\\].\\[name\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\[Day Name\\].+\\[(\\d+)\\].+"
                                                    ,  "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[program\\].\\[program\\].+(\\[Prog Name\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\[Day Name\\].+\\[(\\d+)\\].+"        
                                                    , */   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[affiliation\\].\\[affiliation_admedia\\].+(\\[admedia key\\]).+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+\\[partner\\].\\[Url\\].+(\\[\\d+\\]).+"//(\\[Prog Name\\]).+\\[(\\d+)\\].+\\[Day Name\\].+\\[(\\d+)\\].+"
                                                    /*,   "\"CUBE\",\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}).+\\[Day Name\\].+\\[(\\d+)\\].+\\[Day Name\\].+\\[(\\d+)\\].+\"(\\d+)\".+\"L-JBOSS-.+WITH MEMBER.+\\[Measures\\].(\\[.+\\]).+"};//\",\"(\\d+).+\\[Measures\\].(.+).*"};//.\\[User Name\\].&\\[(.\\d+)\\].*"*/};
// [partner].[partner].[Url].&[2077202]},  {[program].[program].[Prog Name].&[14936]},  {[time].[Day Name].&[20150724]}
    static Pattern[]    PATTERNS    = new Pattern[REGEXS.length];

    static {
            for (int i = 0; i < REGEXS.length; i++){
                PATTERNS[i] = Pattern.compile(REGEXS[i]);
            }
        }
    
    static void openCube() throws IOException {

        PrintWriter output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_cube.csv");
        
        try (BufferedReader cube = new BufferedReader(new FileReader("/Volumes/HD/htdocs/GitHub/log_stuff/log_cube.csv")))
            {
                String sCurrentLine
                    ,   INPUT_STRING
                    ,   timestamp      
                    ,   range_filter_f 
                    ,   range_filter_t 
                    ,   term_filter    
                    ,   group          
                    ,   sorting  
                    ,   order
                    ,   values         
                    ,   input
                    ,   check
                    ;
                int i;
                
                output.println("System;TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");
                
                while ((sCurrentLine = cube.readLine()) != null) {
                    
                    INPUT_STRING    =   sCurrentLine;
                    i = 0;

//                        System.out.println(INPUT_STRING);
                    for (Pattern pattern : PATTERNS) {
                       
                        Matcher matcher = pattern.matcher(INPUT_STRING);
//                        System.out.println(matcher.matches());
                        if (matcher.matches()){
                            i = 1;
                            timestamp       =   matcher.group(1);
                            
//                            if(matcher.group(3).length() ==0){
//                                range_filter_f  =   matcher.group(2);
//                            }
//                            else{
                                
//                            }
                                System.out.println(matcher.group(2));
                            if(matcher.group(2).matches("\\d+")) {  
                                range_filter_f  =   matcher.group(3);//+";"+matcher.group(3);
                                range_filter_t  =   matcher.group(4); 
                                term_filter     =   matcher.group(5);
                                group           =   matcher.group(2);
                                sorting         =   "";//matcher.group(6);
                                order           =   "";//matcher.group(7);
                                values          =   matcher.group(4);
                            }
                            else{   
                                    
                                if(matcher.group(2).matches("[admedia key]")) { 
                                    System.out.println("XXXX");
                                    range_filter_f  = matcher.group(7);
                                    range_filter_t  = "";
                                    term_filter     =   matcher.group(3)+", "+matcher.group(5)+", "+matcher.group(6);;
                                    group           =   matcher.group(2);
                                    sorting         =   "";//matcher.group(6);
                                    order           =   "";//matcher.group(7);
                                    values          =   matcher.group(4);

                                }
                                else{
                                    System.out.println("YYYY");
                                    range_filter_f  =   matcher.group(5);
                                    range_filter_t  =   "";
                                    term_filter     =   matcher.group(3);
                                    group           =   matcher.group(2);
                                    sorting         =   "";//matcher.group(6);
                                    order           =   "";//matcher.group(7);
                                    values          =   matcher.group(4);
                                    }
                                
                            }
//                            
                            
                                input   =   "CUBE;"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";"+order+";'"+values+"'";
                            
//                            System.out.println(input);
                            output.println(input); 
                            
                        }
                        else{
//                            System.out.println(INPUT_STRING);
//                             output.println(INPUT_STRING);
                        }
                    
//                    String  edit;    
//                            
//                            edit    =	sCurrentLine.replace( ",,,", ";;;")
//                                            .replace( ",,", ";;")
//						.replace( "\",\"", ";");
//                    
//                    String[] parts          =   edit.split(";");
//                    
//                    String  timestamp       =   parts[1]
//                        ,   range_filter_f  =   parts[2]
//                        ,   range_filter_t  =   parts[3]
//                        ,   term_filter     =   parts[4]
//                        ,   group           =   parts[5]
//                        ,   sorting         =   parts[7]
//                        ,   values          =   parts[8]
//                        ,   input
//                        ;
//
//                    if( (range_filter_f.length()==0) && (range_filter_t.length()==0) ) {
//                         term_filter  = parts[4];
////                         HIER MUSS NOCH DAS REGEX FÜR DIE DATEN REIN... DAMIT WENN VON BIN DATUM IN DER VARIABLE IST, 
////                         DANN NEHM DIE ZU RANGE_FILTER_F AND RANGE_FILTER_T
//                         
////                        if(term_filter.matches("*.$[0-9]{8})$.*")) /*[0-9]{8}$.**/
////                        {
////                            term_filter  = "CHECK";
////                        }
//                    }
//////                    
//                    input       =   ("'CUBE';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";'"+values+"'");
////                    System.out.println(Arrays.toString(parts));
////                    System.out.println(term_filter);
//                    output.println(input);
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
