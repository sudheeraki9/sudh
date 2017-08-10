/**
 * 
 */
package com.utils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * @author grayzeng
 *
 */
public class SchemaField implements Serializable {
	private static final long serialVersionUID = 1819103920199665667L;
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat sdf_1 = new SimpleDateFormat("MM-dd-yyyy");
	private static final SimpleDateFormat sdf_2 = new SimpleDateFormat("yyyyMMdd");
	private static final SimpleDateFormat sdf_transform = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final Pattern[] datePatterns = { 
			Pattern.compile("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}( UTC)?"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3} ?\\d{2}:\\d{2}"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}( UTC)?"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1}( UTC)?"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{2}( UTC)?"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}Z"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{6}"),
			Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3} [+-]\\d{2}:\\d{2}")
	};
	
	private static final Pattern validDatePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
	private static final Pattern validDatePattern1 = Pattern.compile("\\d{2}/\\d{2}/\\d{4}");
	private static final Pattern validDatePattern2 = Pattern.compile("\\d{8}");
	
	public static enum DataType {
		STRING, BYTES, INTEGER, FLOAT, BOOLEAN, TIMESTAMP;
	}

	public static enum FieldMode {
		NULLABLE, REQUIRED;
	}

	private String name;
	private DataType type;
	private FieldMode mode;
	private String description;
	
	public SchemaField() {}

	public SchemaField(String name) {
		super();
		this.name = name;
		this.type = DataType.STRING;
		this.mode = FieldMode.NULLABLE;
		this.description = description;
	}

	public SchemaField(String name, DataType type, FieldMode mode, String description) {
		super();
		this.name = name;
		if (type != null)
			this.type = type;
		else
			this.type = DataType.STRING;
		if (mode != null)
			this.mode = mode;
		else
			this.mode = FieldMode.NULLABLE;
		this.description = description;
	}

	public Object isValidate(String input) throws Exception {
		String target = null;
		if (StringUtils.isNotBlank(input)) {
			target = input.trim();
			// remove begin "
			if (target.startsWith("\"")) {
				target = target.substring(1);
			}
			// remove begin "
			if (target.endsWith("\"")) {
				target = target.substring(0, target.length() - 1);
			}
			// replace double " with single "
			if (StringUtils.isNotBlank(target)) {
				target = target.replace("\"\"", "\"");
			}
		}

		if (StringUtils.isBlank(target) || target.equalsIgnoreCase("null")) {
			if (this.mode == FieldMode.NULLABLE) {
				return null;
			} else {
				throw new Exception("Missed required field");
			}
		}

		if (target.equalsIgnoreCase(this.name)) {
			throw new Exception("Ignored Header");
		}
		
		if (target.equalsIgnoreCase(this.description)) {
			throw new Exception("Ignored Description");
		}

		if (this.type == DataType.INTEGER) {
			try {
				return Integer.parseInt(target);
			} catch (NumberFormatException e) {
				throw new Exception(String.format("Invalid INTEGER (%s)", target));
			}
		} else if (this.type == DataType.FLOAT) {
			try {
				return Float.parseFloat(target);
			} catch (Exception e) {
				throw new Exception(String.format("Invalid FLOAT (%s)", target));
			}
		} else if (this.type == DataType.BOOLEAN) {
			if ("true".equals(target) || "1".equals(target))
				return true;
			else if ("false".equals(target) || "0".equals(target))
				return false;
			else
				throw new Exception(String.format("Invalid BOOLEAN (%s)", target));
		} else if (this.type == DataType.TIMESTAMP) {
			for (Pattern p : datePatterns){
				if (p.matcher(target).matches()){
					return target;
				}
			}
			if (validDatePattern.matcher(target).matches()){
				Date date = sdf.parse(target);
				if (date != null) {
					return sdf_transform.format(date);
				}
			}
	//Add new date format		
			if (validDatePattern1.matcher(target).matches()){
				Date date = sdf_1.parse(target);
				if (date != null) {
					return sdf_transform.format(date);
				}
			}
			if (validDatePattern2.matcher(target).matches()){
				Date date = sdf_2.parse(target);
				if (date != null) {
					return sdf_transform.format(date);
				}
			}
			throw new Exception(String.format("Invalid Date (%s)", target));
		} else if (this.type == DataType.STRING) {
			//Replace Star Example
			target = target.replace("*", "");
		}
		return target;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}

	public FieldMode getMode() {
		return mode;
	}

	public void setMode(FieldMode mode) {
		this.mode = mode;
	}
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}


}
