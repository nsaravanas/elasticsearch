package com.example;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.PreDestroy;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ExcelToES implements CommandLineRunner {

	@Autowired
	private TransportClient client;

	public void main(String... args) throws Exception {
		SpringApplication.run(ExcelToES.class, args);
	}

	@Override
	public void run(String... args) {
		String index = null;
		String file1 = null;
		String source1 = null;
		String file2 = null;
		String source2 = null;
		if (args.length != 5 && args.length != 3) {
			System.out.println("Usage: indexName odhFile dmFile");
			System.out.println("or");
			System.out.println("Usage: indexName odhFile odhKey dmFile dmKey");
			System.exit(0);
		}
		if (args.length == 3) {
			index = args[0];
			file1 = args[1];
			source1 = "odh";
			file2 = args[2];
			source2 = "dm";
		}
		if (args.length == 5) {
			index = args[0];
			file1 = args[1];
			source1 = args[2];
			file2 = args[3];
			source2 = args[4];
		}
		ExcelToES ejson = new ExcelToES();
		boolean success1 = ejson.processExcel(index, source1, file1);
		boolean success2 = ejson.processExcel(index, source2, file2);
		if (!success1 || !success2) {
			System.err.println("some error occurred");
		}
		ejson.close();
	}

	private boolean processExcel(String index, String source, String filePath) {
		int counter = 0;
		try {
			FileInputStream fis = new FileInputStream(filePath);
			Workbook workbook = new HSSFWorkbook(fis);
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
				StringBuilder sb = new StringBuilder();
				sb.append("{ ");
				for (int i = 0; i < header.size(); i++) {
					Cell cell = row.getCell(i);
					Object obj = getValue(cell);
					sb.append("\"").append(header.get(i)).append("\" : \"").append(obj).append("\" , ");
				}
				sb.append("\"source\" : \"" + source + "\" } ");
				this.indexDocument(index, source, ++counter + "", sb.toString());
				sb.setLength(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private static Object getValue(Cell cell) {
		Object value = null;
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_STRING:
			value = cell.getRichStringCellValue();
			break;
		case Cell.CELL_TYPE_NUMERIC:
			if (DateUtil.isCellDateFormatted(cell)) {
				Date date = cell.getDateCellValue();
				value = new SimpleDateFormat("yyyy-MM-dd").format(date);
			} else {
				value = cell.getNumericCellValue();
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

	@PreDestroy
	public void close() {
		client.close();
	}

	private void indexDocument(String index, String type, String id, String source) {
		client.prepareIndex(index, type, id).setSource(source).get().getIndex();
	}
}