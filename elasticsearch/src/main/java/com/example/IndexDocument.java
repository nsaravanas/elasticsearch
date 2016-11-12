package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.gson.Gson;

@SpringBootApplication
public class IndexDocument implements CommandLineRunner {

	private Client client;

	public void init() {
		Settings settings = Settings.settingsBuilder().build();
		try {
			this.client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(IndexDocument.class, args);
	}

	@Override
	public void run(String... args) throws IOException {
		String index = null;
		String file1 = null;
		String type1 = null;
		String file2 = null;
		String type2 = null;
		if (args.length != 5 && args.length != 3) {
			System.out.println("Usage: indexName odhFile dmFile");
			System.out.println("or");
			System.out.println("Usage: indexName odhFile odhKey dmFile dmKey");
			System.exit(0);
		}
		if (args.length == 3) {
			index = args[0];
			file1 = args[1];
			type1 = "odh";
			file2 = args[2];
			type2 = "dm";
		}
		if (args.length == 5) {
			index = args[0];
			file1 = args[1];
			type1 = args[2];
			file2 = args[3];
			type2 = args[4];
		}

		IndexDocument ejson = new IndexDocument();
		ejson.init();
		boolean success = ejson.processFile(index, type1, file1);
		if (!success) {
			System.err.println("Something went wrong while processing " + file1 + " , Check log for error.");
		} else {
			System.out.println("Succesfully indexed document " + file1);
		}
		success = ejson.processFile(index, type2, file2);
		if (!success) {
			System.err.println("Something went wrong while processing " + file2 + " , Check log for error.");
		} else {
			System.out.println("Succesfully indexed document " + file2);
		}
		ejson.close();
	}

	private boolean processFile(String index, String type, String filePath) {
		String file = filePath.trim().toLowerCase();
		if (file.endsWith(".xls") || file.endsWith(".xlsx")) {
			return processExcel(index, type, filePath);
		} else if (file.endsWith(".csv")) {
			return processCSV(index, type, filePath);
		} else {
			throw new IllegalArgumentException("Only xls, xlsx and csv file formats are supported.");
		}
	}

	private boolean processCSV(String index, String type, String filePath) {

		char delimiter = ',';
		int counter = 0;

		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			String head = br.readLine().toLowerCase();
			Map<Integer, Long> freqMap = head.replaceAll("[a-zA-Z]", "").chars().boxed()
					.collect(Collectors.groupingBy(i -> i, Collectors.counting()));
			delimiter = (char) freqMap.entrySet().stream().sorted(Map.Entry.comparingByValue()).findFirst().get().getKey().intValue();
		} catch (IOException e) {
			e.printStackTrace();
		}

		CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter);
		boolean ignore = false;
		List<String> header = new ArrayList<>();
		try {
			CSVParser parser = CSVParser.parse(new File(filePath), Charset.defaultCharset(), format);
			for (CSVRecord record : parser) {
				if (!ignore) {
					for (String head : record) {
						header.add(head.toLowerCase());
					}
					ignore = true;
					continue;
				}
				Map<String, String> jsonMap = new LinkedHashMap<>();
				for (int i = 0; i < header.size(); i++) {
					jsonMap.put(header.get(i), record.get(i));
				}
				jsonMap.put("id", (++counter) + "");
				this.indexDocument(index, type, jsonMap);
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	private boolean processExcel(String index, String type, String filePath) {
		int counter = 0;
		try {
			InputStream is = new FileInputStream(filePath);
			Workbook workbook = WorkbookFactory.create(is);
			Sheet sheet = workbook.getSheetAt(0);
			List<String> header = new ArrayList<String>();
			Row headerRow = sheet.getRow(0);
			for (Cell cell : headerRow) {
				header.add(cell.getRichStringCellValue().getString().toLowerCase());
			}
			boolean ignore = false;
			for (Row row : sheet) {
				if (!ignore) {
					ignore = true;
					continue;
				}
				Map<String, Object> jsonMap = new HashMap<>();
				for (int i = 0; i < header.size(); i++) {
					Cell cell = row.getCell(i);
					Object obj = getValue(cell);
					jsonMap.put(header.get(i), obj);
				}
				jsonMap.put("source", type);
				jsonMap.put("id", ++counter);
				this.indexDocument(index, type, jsonMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private void indexDocument(String index, String type, Map<String, ?> jsonMap) {
		String json = null;
		try {
			json = new Gson().toJson(jsonMap);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error while converting to JSON " + jsonMap);
		}
		try {
			String in = this.indexDocument(index, type, jsonMap.get("id") + "", json);
			System.out.println("Indexed document => " + index + "::" + type + "::" + in);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error while indexing json document " + jsonMap);
		}
	}

	private static Object getValue(Cell cell) {

		Object value = "";
		if (cell == null) {
			return "";
		}

		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_STRING:
			value = cell.getRichStringCellValue().toString();
			break;
		case Cell.CELL_TYPE_NUMERIC:
			if (DateUtil.isCellDateFormatted(cell)) {
				Date date = cell.getDateCellValue();
				value = new SimpleDateFormat("yyyy-MM-dd").format(date);
			} else {
				value = NumberToTextConverter.toText(cell.getNumericCellValue());
			}
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			value = cell.getBooleanCellValue();
			break;
		default:
			value = "";
		}
		return value;
	}

	private String indexDocument(String index, String type, String id, String source) {
		IndexResponse response = client.prepareIndex(index, type, id).setSource(source).get();
		return response.getId() + "::" + response.getVersion();
	}

	public void close() {
		client.close();
	}
}