package org.h2.command.dml;

import java.util.regex.Pattern;

public class RecursiveQuery {
	
	// A query is recursive if it references it's own name in its definition
	public static boolean isRecursive(String tempViewName, String querySQL) {
		return Pattern.matches("\\b"+tempViewName+"\\b",querySQL);
	}

}
