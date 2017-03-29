package com.hiveudaf.groupconcat;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
 
import java.util.HashMap;
import java.util.TreeSet;
 
/**
 * This class will concatenate the rows under a column, using another column to sort the rows before concatenating.
 * The order can also be passed as an argument.
 * The delimiter to be used while concatenating can also be passed. By default "," will be used as delimiter
 * 
 * @author Ashish George
 */
public final class GroupConcatWithSort extends UDAF
{
    public static class Evaluator implements UDAFEvaluator
    {
        private HashMap<Integer, String> inputMap;
        private String uniqueSeparator;
 
        public Evaluator()
        {
            init();
        }
 
        /**
         * Initializes the evaluator and resets its internal state.
         */
        public void init()
        {
        	inputMap = new HashMap<Integer, String>();
        	uniqueSeparator="###13490###";
        }
 
        /**
         * This function is called every time there is a new value to be aggregated.
         * The parameters are the same parameters that are passed when function is called in Hive query.
         *
         * @param key Integer
         * @param value String
         * @return Boolean
         */
        public boolean iterate(Integer key, String value,String delimiter)
        {
         if(null==value)//handling null
           value="";
        	System.out.println("~~~~~~~~~~~ inside iterator ~~~~~~~~~~~");
            if(!inputMap.containsKey(key)) {
            	System.out.println("key-> "+key+" Input--> "+ delimiter+uniqueSeparator+value);
           		inputMap.put(key, delimiter+uniqueSeparator+value);
            }
            return true;
        }
 
        /**
         * Function called when separated jobs are done on different data nodes (partial aggregation)
         *
         * @return HashMap
         */
        public HashMap<Integer, String> terminatePartial()
        {
        	System.out.println("~~~~~~~~~~~ inside terminatePartial ~~~~~~~~~~~");
            return inputMap;
        }
 
        /**
         * Function called when merging all data result calculated from all data notes
         *
         * @param another HashMap
         * @return Boolean
         */
        public boolean merge(HashMap<Integer, String> another)
        {
        	System.out.println("~~~~~~~~~~~ inside merge ~~~~~~~~~~~");
            //null might be passed in case there is no input data.
            if (another == null) {
                return true;
            }
 
            for(Integer key : another.keySet()) {
                if(!inputMap.containsKey(key)) {
                	inputMap.put(key, another.get(key));
                }
            }

            return true;
        }
 
        /**
         * This function is called when the final result of the aggregation is needed
         *
         * @return String
         */
        public String terminate()
        {
        	System.out.println("~~~~~~~~~~~ inside merge ~~~~~~~~~~~");
        	String concatinatedString="";
            if (inputMap.size() == 0) {
                return null;
            }
            TreeSet<Integer> sortedKeySet=new TreeSet<Integer>();
            sortedKeySet.addAll(inputMap.keySet());
            boolean isFirst=true;
            System.out.println("~~~~~~~~~ inside final sorting per row ~~~~~~~~~");
            for(int key : sortedKeySet){
            	String tobeConcatinated=inputMap.get(key);
            	System.out.println(tobeConcatinated);
            	if(!isFirst){
            		tobeConcatinated=tobeConcatinated.replaceFirst(uniqueSeparator, "");
            	}
            	else{
            		if(tobeConcatinated.split(uniqueSeparator).length>1)
            			tobeConcatinated=tobeConcatinated.split(uniqueSeparator)[1];
            		else
            			tobeConcatinated=tobeConcatinated.split(uniqueSeparator)[0];
            		isFirst=false;
            	}
            		
            	
            	concatinatedString+=tobeConcatinated;
            }                	
            
            return concatinatedString;
        }
    }
}
