/**
 * 
 */
package com.utils;

import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * @author grayzeng
 *
 */
public class SchemaUtils {
	/*public static String getSchemaDefFromResource(String schemaDef_filename) throws Exception {
		BufferedReader reader = null;
		try {
			InputStream in = SchemaUtils.class.getResourceAsStream("/" + schemaDef_filename);
			//File file = new File("..//war//Nullable_COMMODITY_WEEKLY_ALL.txt");
			File file = new File("..//war//PROMO.txt");
			//InputStream in = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(in));

			StringBuilder out = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				out.append(line);
			}
			return out.toString();
		} catch (Exception e) {
			throw e;
		} finally {
			if (reader != null)
				reader.close();
		}

	}*/
	/*
	public static String getSchemaDefFromFile(String fielPath, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(fielPath));
		return new String(encoded, encoding);
	}*/

    public static TableSchema getErroLogSchema() {
        return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
			private static final long serialVersionUID = 362986892691423945L;

			{
				add(new TableFieldSchema().setName("FILE_NAME").setType("STRING"));
				add(new TableFieldSchema().setName("KEY").setType("STRING"));
                add(new TableFieldSchema().setName("Source").setType("STRING"));
                add(new TableFieldSchema().setName("Error_Description").setType("STRING"));
              }
        });
      }

    public static TableRow getErrorLogTableRow(String filename, String key, String source, String errors) {
        return new TableRow().set("FILE_NAME", filename)
        					.set("KEY", key)
        					.set("Error_Description", errors)
        					.set("Source", source);
    }

}
