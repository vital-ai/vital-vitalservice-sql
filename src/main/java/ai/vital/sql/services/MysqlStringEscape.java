package ai.vital.sql.services;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 * SOURCE:
 * http://stackoverflow.com/questions/881194/how-to-escape-special-character-in-
 * mysql/6478616#6478616 Important, NO_BACKSLASH_ESCAPES must be disabled
 * http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#
 * sqlmode_no_backslash_escapes
 * 
 *
 */
public class MysqlStringEscape {

	private static final String MYSQL_ESCAPE_STRINGS_TXT = "mysql_escape_strings.txt";
	private static final HashMap<String, String> sqlTokens;
	private static Pattern sqlTokenPattern;
	
	static {
		
		//ugly fix for groovy doc issue with unicode 0000 character
		List<String[]> records = new ArrayList<String[]>();
		InputStream inputStream = null;
		try {
			inputStream = MysqlStringEscape.class.getResourceAsStream(MYSQL_ESCAPE_STRINGS_TXT);
			
			int i = 0;
			
			for(String l : IOUtils.readLines(inputStream, "UTF-8") ) {
				
				i++;
				l = l.trim();
				if(l.isEmpty()) continue;
				
				String[] split = l.split("\t");
				
				if(split.length != 3) throw new RuntimeException("Invalid " + MYSQL_ESCAPE_STRINGS_TXT + " file, line: " + i + ": should have 3 tsv columns");
		
				for(int j = 0 ; j < split.length; j++) {
					split[j] = StringEscapeUtils.unescapeJava(split[j]);
				}
				records.add(split);
			}
			
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
		
		/* uncomment it to test
		//replace U with single backslash+u !
		// MySQL escape sequences:
		// http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
		String[][] search_regex_replacement = new String[][] {
			// search string search regex sql replacement regex
			{ "U0000", "\\x00", "\\\\0" }, 
			{ "'", "'", "\\\\'" }, 
			{ "\"", "\"", "\\\\\"" },
			{ "\b", "\\x08", "\\\\b" }, 
			{ "\n", "\\n", "\\\\n" }, 
			{ "\r", "\\r", "\\\\r" },
			{ "\t", "\\t", "\\\\t" }, 
			{ "\u001A", "\\x1A", "\\\\Z" }, 
			{ "\\", "\\\\", "\\\\\\\\" }
			// \%A "%" character; see note following the table
			// \_A "_" character; see note following the table
		};

		if(search_regex_replacement.length != records.size()) throw new RuntimeException("size is different");
		for(int i = 0 ; i < search_regex_replacement.length; i++) {
			String[] r = search_regex_replacement[i];
			String[] r2 = records.get(i);
			
			if( ! Arrays.deepEquals(r, r2) ) throw new RuntimeException("Records broken: " + r + " vs " + r2);
		}
		*/
		
		sqlTokens = new HashMap<String, String>();
		String patternStr = "";
		for (String[] srr : /*search_regex_replacement*/ records) {
			sqlTokens.put(srr[0], srr[2]);
			patternStr += (patternStr.isEmpty() ? "" : "|") + srr[1];
		}
		sqlTokenPattern = Pattern.compile('(' + patternStr + ')');
	}
	
	public static String escape(String s) {
		Matcher matcher = sqlTokenPattern.matcher(s);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(sb, sqlTokens.get(matcher.group(1)));
		}
		matcher.appendTail(sb);
		return sb.toString();
	}
}
