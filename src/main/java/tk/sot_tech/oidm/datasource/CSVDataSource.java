/*
 * Copyright (c) 2016, eramde
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package tk.sot_tech.oidm.datasource;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import tk.sot_tech.oidm.sch.AbstractDataSource;
import tk.sot_tech.oidm.sch.SheduledTaskTerminatedException;
import tk.sot_tech.oidm.utility.Misc;

public class CSVDataSource extends AbstractDataSource {

	public static final int NONE = 0, STORE = 1, FIELD = 2, LINE = 3, ERROR = -1;
	public static final char CAR_RET = '\r', NEW_LINE = '\n', QUOTE = '"', FIELD_SEPARATOR = ',';
	public static final String FILE_PATH_PARAMETER = "filePath",
		FILE_ENCODING_PARAMETER = "codePage";

	public static enum Token {
		CHAR, FIELD_SEPARATOR, LINE_SEPARATOR, CAR_RETURN, QUOTE;
	}

	public final int[][] AUTOMATON_STATUM = {
		{3, 0, 0, 0, 1},
		{1, 1, 1, 1, 2},
		{ERROR, 0, 0, 2, 1},
		{3, 0, 0, 3, ERROR}
	}, AUTOMATON_MODUM = {
		{STORE, FIELD, LINE, NONE, NONE},
		{STORE, STORE, STORE, STORE, NONE},
		{ERROR, FIELD, LINE, NONE, STORE},
		{STORE, FIELD, LINE, NONE, ERROR}
	};

	protected boolean expectHeader = false;

	protected int state, column, symbol, lines, maxColumns = 0;
	protected HashMap<String, Object> readyLine;
	protected ArrayList<String> headers;
	protected StringBuilder readyField;
	protected final ArrayList<HashMap<String, Object>> parsedLines = new ArrayList<>();
	private String filePath, fileEncoding = "UTF-8";

	@Override
	public ArrayList<HashMap<String, Object>> fetchData() throws Exception,
																 SheduledTaskTerminatedException {

		String data = new String(Files.readAllBytes(Paths.get(filePath)), Charset.forName(
								 fileEncoding));
		if (Misc.isNullOrEmpty(data)) {
			process(data);
		}
		return parsedLines;
	}

	public void process(String data) {
		data = data.endsWith("" + NEW_LINE) ? data : data + NEW_LINE;
		for (char c : data.toCharArray()) {
			++symbol;
			Token token = getTokenType(c);
			performAction(token, c);
			state = AUTOMATON_STATUM[state][token.ordinal()];
		}
		checkEnding();
	}

	//XXX: if table is pyramyd-style, there's a problem...
	private HashMap<String, Object> maximize(HashMap<String, Object> readyLine) {
		HashMap<String, Object> ret = readyLine;
		if (ret.size() < maxColumns) {
			for (int i = ret.size(); i < maxColumns; ++i) {
				ret.put(headers.get(i), "null");
			}
		}
		return ret;
	}

	protected Token getTokenType(char c) {
		Token type = Token.CHAR;
		switch (c) {
			case FIELD_SEPARATOR:
				type = Token.FIELD_SEPARATOR;
				break;
			case NEW_LINE:
				type = Token.LINE_SEPARATOR;
				break;
			case CAR_RET:
				type = Token.CAR_RETURN;
				break;
			case QUOTE:
				type = Token.QUOTE;
				break;
			default:
				break;
		}
		return type;
	}

	private String generateHeader(String proposed) {
		if (expectHeader && lines == 0) {
			return proposed;
		}
		String ret;
		int i = column;
		while (headers.contains(ret = Integer.toString(i))) {
			++i;
		}
		return ret;
	}

	public CSVDataSource checkEnding() {
		if (readyField.length() > 0) {
			String tmp = readyField.toString();
			if (headers.size() - 1 < column) {
				headers.add(generateHeader(tmp));
			}
			++column;
			if (!expectHeader || lines > 0) {
				String name = headers.get(column - 1);
				readyLine.put(name, tmp);
			}
		}
		if (!readyLine.isEmpty()) {
			parsedLines.add(readyLine);
		}
		return this;
	}

	protected void performAction(Token token, char c) {
		int a = AUTOMATON_MODUM[state][token.ordinal()];
		switch (a) {
			case STORE:
				readyField.append(c);
				break;
			case LINE:
			case FIELD:
				String s = readyField.toString();
				readyField = new StringBuilder();
				if (headers.size() - 1 < column) {
					headers.add(generateHeader(s));
				}
				++column;
				if (!expectHeader || lines > 0) {
					String name = headers.get(column - 1);
					readyLine.put(name, s);
					if (a == LINE) {
						parsedLines.add(maximize(readyLine));
						readyLine = new LinkedHashMap<>();
					}
				}
				if (a == LINE) {
					++lines;
					if (column > maxColumns) {
						maxColumns = column;
					}
					column = 0;
				}
				break;
			case ERROR:
				throw new IllegalStateException("Unable to parse CSV. Symbol #"
												+ symbol + " ('" + c + "'): token "
												+ token + " is illegal here");
		}
	}

	@Override
	protected AbstractDataSource initImpl() {
		filePath = parameters.getParameters().get(FILE_PATH_PARAMETER);
		File file = new File(filePath);
		if (!file.exists() || !file.canRead()) {
			throw new IllegalArgumentException("Can't read " + filePath);
		}
		String tmp = parameters.getParameters().get(FILE_ENCODING_PARAMETER);
		if (!Misc.isNullOrEmpty(tmp)) {
			fileEncoding = tmp;
		}
		return this;
	}

	@Override
	public void clearData(ArrayList<HashMap<String, Object>> values) throws Exception,
																			SheduledTaskTerminatedException {

	}

	@Override
	public void close() throws Exception {

	}

}
