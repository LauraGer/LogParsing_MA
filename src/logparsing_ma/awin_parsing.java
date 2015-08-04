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
class awin_parsing {
    
         //(USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)
    static String       FILE        =  "/Volumes/HD/X/LOGS/awin_edit.csv";
    
    static String[]     REGEXS      = new String[] {".+\\d+\",\"(.+)\",\".+"};//(\\d{4}-\\d{2}-\\d{2}\\d{2}:\\d{2}:\\d{2})\",\"(\\d{4}-\\d{2}-\\d{2})\",\"(\\d{4}-\\d{2}-\\d{2})\",\"(.+)([DESC|ASC]).+"};//\",\"[.+.(\\d)|([a-z].[a-z])]\",\"(.+)\",\".+,\"(.+)FROM.*"};
//    "5477439","commissionsReport","2015-06-1711:23:23","2015-03-24","2015-03-26","commission_amountDESC","affiliate.54760","transaction",400,"SELECTSQL_CALC_FOUND_ROWSTCR.id,TCR.status,TCR.merchant_id,TCR.program_name,TCR.type,TCR.sale_amount,TCR.commission_amount,TCR.count,TCR.click_date,TCR.trans_date,TCR.valid_date,TCR.click_ref,TCR.is_searchsite_id,TCR.click_through_phrase,TCR.hide_details,TCR.platform,TCR.is_invoice_restricted,TCR.debter_level,TCR.invoice_paid,TCR.url,TCR.old_sale_amount,TCR.old_commission,TCR.device_name,TCR.payment_status,TCR.risk_level,ps.nameFROM`affiliatewindow`.`temp_commissions_report_a54760`TCRLEFTJOIN`affiliatewindow`.`merchant_payment_status`mpsON(TCR.merchant_id=mps.merchant_id)LEFTJOIN`affiliatewindow`.`payment_status`psON(ps.id=mps.payment_status_id)INNERJOIN`affiliatewindow`.`memberships`memONTCR.merchant_id=mem.merchant_idAND54760=mem.affiliate_idWHERETCR.platform!=\"sw\"ORDERBYcommission_amountDESCLIMIT0,400"
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
                    
                    for (Pattern pattern : PATTERNS) {
                        
                        Matcher matcher = pattern.matcher(sCurrentLine);
                        
                        if (matcher.matches()){
                            
                            timestamp       =   matcher.group(1);
                            range_filter_f  =   "";//matcher.group(4);
                            range_filter_t  =   "";//matcher.group(5);
                            term_filter     =   "";// matcher.group(2) +" : "+ matcher.group(3);
                            group           =   "";//matcher.group(6);
                            sorting         =   "";//matcher.group(8);
                            order           =   "";//matcher.group(9);
                            values          =   "";//matcher.group(7);
                        
                            input       =   ("'AWIN';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";'"+values+"'");
                        
                            output.println(input);
                        }
                        else{
                            System.out.println(sCurrentLine);
                        }
                        
                    }    
                    
                
//                    String[] parts          =   sCurrentLine.split("','");
//                    
//                    String  timestamp       =   parts[2]
//                        ,   range_filter_f  =   parts[3]
//                        ,   range_filter_t  =   parts[4]
//                        ,   term_filter     =   parts[6].replaceAll("(affiliate.)([0-9]{1,9})","$2")
//                        ,   group           =   parts[7]
//                        ,   sorting         =   parts[5].replaceAll("DESC", "';'DESC").replaceAll("ASC", "';'ASC")
//                        ,   values          =   parts[9]
//                        ,   input
//                        ;
//                    
//                    int     value_index     =   values.indexOf("FROM")+1;
//                    
//                    values      =   values.substring(0,value_index);
//                        
//                    input       =   ("'AWIN';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";'"+values+"'");
//                    
//                    
//                    if(parts[0].matches("54\\d{5}|[0-9]{7}'"))
//                    {
//                        output.println(input);
////                    System.out.println(input);
////                    System.out.println(Arrays.toString(parts));
//                    }
                }

        } catch (IOException e) {
        } 
        output.close();
        System.out.println("AWIN done!");
    }
}
