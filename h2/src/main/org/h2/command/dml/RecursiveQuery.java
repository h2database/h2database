package org.h2.command.dml;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RecursiveQuery {
	
	// A query is recursive if it references it's own name in its definition
	public static boolean isRecursive(String tempViewName, String querySQL) {
		// ?i is case insensitive
		// ?m is multi-line search
		// ?d is Unix line endings
		String pattern = "(?i)(?m)(?d).*\\b("+tempViewName+")\\b";
		System.out.println("pattern="+pattern);
		boolean stringContains = querySQL.contains(tempViewName);
		System.out.println("stringContains="+stringContains);
		boolean foundAny = RecursiveQuery.foundAny(tempViewName,querySQL);
		System.out.println("foundAny="+foundAny);
		boolean patternMatch = Pattern.matches(pattern,querySQL);
		System.out.println("patternMatch="+patternMatch);
		return patternMatch||stringContains|| foundAny;
	}

	private static boolean foundAny(String tempViewName, String querySQL){
		Pattern p = Pattern.compile("(?i)(?m)(?d)\\b("+tempViewName+")\\b");
		Matcher m = p.matcher(querySQL);
		while (m.find()) {
		   return true;
		}		
		return false;
	}
}
