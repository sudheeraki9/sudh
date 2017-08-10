/**
 * 
 */
package com.utils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
//import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.gson.Gson;
import com.google.common.base.Splitter;
import com.google.common.reflect.TypeToken;
import com.utils.SchemaField.DataType;
import com.utils.SchemaField.FieldMode;


/**
 * @author grayzeng
 *
 */
public class SchemaDef implements Serializable {
	private static final long serialVersionUID = -5514854377249637082L;
	
	public static enum DelimiterType {
		COMMAS, PIPE, SLASH, COMMA,SEMICO,COMQO;
	}
	
//	public static final Splitter commas_splitter = Splitter.on(Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
//	public static final Splitter pipe_splitter = Splitter.on(Pattern.compile("\\|(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
	public static final Splitter commas_splitter = Splitter.on(Pattern.compile("\",\""));
	public static final Splitter pipe_splitter = Splitter.on(Pattern.compile("\\|"));
	public static final Splitter slash_splitter = Splitter.on(Pattern.compile("\"\\/\""));
	public static final Splitter comma_splitter = Splitter.on(Pattern.compile(","));
	public static final Splitter comqo_splitter = Splitter.on(Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
	public static final Splitter semico_splitter = Splitter.on(Pattern.compile(";"));
	private List<SchemaField> fields = new ArrayList<SchemaField>();
	private DelimiterType delimiter;



	public SchemaDef(DelimiterType delimiter, String jsonArrayString) throws IOException {
		if (delimiter != null){
			this.delimiter = delimiter;
		}
		else {
			this.delimiter = DelimiterType.COMMAS;
		}
		
		Gson gson = new Gson();
		Type collectionType = new TypeToken<List<SchemaField>>() {
			
		private static final long serialVersionUID = -3948624483190362017L;}.getType();
		this.fields = gson.fromJson(jsonArrayString, collectionType);
	}
	
	public Splitter getSplitter(){
		if (this.delimiter == DelimiterType.PIPE){
			return SchemaDef.pipe_splitter;
		}
		if (this.delimiter == DelimiterType.SLASH){
			return SchemaDef.slash_splitter;
		}
		if (this.delimiter == DelimiterType.COMMA){
			return SchemaDef.comma_splitter;
		}
		if (this.delimiter == DelimiterType.SEMICO){
			return SchemaDef.semico_splitter;
		}
		if (this.delimiter == DelimiterType.COMQO){
			return SchemaDef.comqo_splitter;
		}
		return SchemaDef.commas_splitter;
	}
	
	public SchemaField getSchemaFieldByPos(int pos) {
		return fields.get(pos);
	}

	public void addFiled(SchemaField field) {
		this.fields.add(field);
	}

	public void addField(String name, DataType type, FieldMode mode, String description) {
		SchemaField field = new SchemaField(name, type, mode, description);
		this.fields.add(field);
	}

	public TableSchema populateTableSchema() {
		ArrayList<TableFieldSchema> tableFieldSchemas = new ArrayList<TableFieldSchema>();
		for (SchemaField field : this.fields) {
			tableFieldSchemas.add(new TableFieldSchema().setName(field.getName()).setType(field.getType().name()).setMode(field.getMode().name()).setDescription(field.getDescription()));
		}

		return new TableSchema().setFields(tableFieldSchemas);
	}


	public List<SchemaField> getFields() {
		return fields;
	}

	public void setFields(List<SchemaField> fields) {
		this.fields = fields;
	}

}
