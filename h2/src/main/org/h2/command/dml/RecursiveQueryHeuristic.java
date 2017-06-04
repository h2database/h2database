package org.h2.command.dml;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RecursiveQueryHeuristic {
	
	// A query is recursive if it references it's own name in its definition
	public static boolean isRecursive(String tempViewName, String querySQL) {
		boolean foundAny = RecursiveQueryHeuristic.foundAny(tempViewName,querySQL);
		//System.out.println("foundAny="+foundAny);
		return foundAny;
	}

	private static boolean foundAny(String tempViewName, String querySQL){
		// ?i is case insensitive
		// ?m is multi-line search
		// ?d is Unix line endings
		Pattern p = Pattern.compile("(?i)(?m)(?d)\\b("+tempViewName+")\\b");
		Matcher m = p.matcher(querySQL);
		while (m.find()) {
		   return true;
		}		
		return false;
	}
}
