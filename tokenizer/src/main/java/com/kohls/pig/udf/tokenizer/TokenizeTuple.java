package com.kohls.pig.udf.tokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.protegrity.pig.udf.ptyProtectStr;
import com.protegrity.pig.udf.ptyPigProtectorException;

public class TokenizeTuple extends EvalFunc<Tuple> {
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	Tuple tuple_for_tokenizer;
	Pattern compiled_pattern;
	ptyProtectStr protectStr;
	Matcher pattern_matcher;
	Tuple tupleToProtect;
	Tuple tuple_columns;
	Tuple tuple_tokens;
	Tuple tuple;

	public TokenizeTuple() {
		try {
			protectStr = new ptyProtectStr();
		} catch (ptyPigProtectorException e) {
			e.printStackTrace();
		}
	}

	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		ArrayList<String> listOfTokens = new ArrayList<String>();
		tuple_for_tokenizer = mTupleFactory.newTuple(2);
		try {
			tuple_columns = (Tuple) input.get(0);
			tuple_tokens = (Tuple) input.get(1);
			tuple = mTupleFactory.newTuple(tuple_columns.size() * 2);
			for (int i = 0; i < tuple_tokens.size(); i++) {
				listOfTokens.add(tuple_tokens.get(i).toString());
			}
			for (int i = 0; i < tuple_columns.size(); i++) {

				if (tuple_columns.get(i) == null || tuple_columns.get(i) == ""
						|| tuple_columns.get(i).toString().length() > 257) {
					tuple.set(i * 2, tuple_columns.get(i));
					tuple.set(i * 2 + 1, "N");
				} else {
					if ("TKN_EMAIL".equals(listOfTokens.get(i).toString()
							.trim().toUpperCase())) {
						String pattern = "(.*)(\\@+)(.*)";
						compiled_pattern = Pattern.compile(pattern);
						pattern_matcher = compiled_pattern
								.matcher(tuple_columns.get(i).toString().trim()
										.toUpperCase());
						if (pattern_matcher.find()) {
							if (pattern_matcher.group(0).length() > 3
									&& pattern_matcher.group(1).length() > 0
									&& pattern_matcher.group(3).length() > 0) {
								tuple_for_tokenizer.set(0, tuple_columns.get(i)
										.toString().trim().toUpperCase());
								tuple_for_tokenizer.set(1, listOfTokens.get(i)
										.toString().trim().toUpperCase());
								tuple.set(i * 2,
										protectStr.exec(tuple_for_tokenizer));
								tuple.set(i * 2 + 1, "Y");
							} else {
								tuple.set(i * 2, tuple_columns.get(i));
								tuple.set(i * 2 + 1, "N");
							}
						} else {
							tuple.set(i * 2, tuple_columns.get(i));
							tuple.set(i * 2 + 1, "N");
						}

					} else if ("TKN_DATE".equals(listOfTokens.get(i).toString()
							.trim().toUpperCase())) {
						String pattern = "^[0-9]*$";
						if (tuple_columns.get(i).toString().trim()
								.toUpperCase().matches(pattern)
								&& tuple_columns.get(i).toString().trim()
										.toUpperCase().length() > 2) {
							tuple_for_tokenizer.set(0, tuple_columns.get(i)
									.toString().trim().toUpperCase());
							tuple_for_tokenizer.set(1, listOfTokens.get(i)
									.toString().trim().toUpperCase());
							tuple.set(i * 2,
									protectStr.exec(tuple_for_tokenizer));
							tuple.set(i * 2 + 1, "Y");
						} else {
							tuple.set(i * 2, tuple_columns.get(i));
							tuple.set(i * 2 + 1, "N");
						}

					} else if ("TKN_INCOME".equals(listOfTokens.get(i)
							.toString().trim().toUpperCase())) {
						String pattern = "^[0-9]*$";
						if (tuple_columns.get(i).toString().trim()
								.toUpperCase().matches(pattern)
								&& tuple_columns.get(i).toString().trim()
										.toUpperCase().length() > 2) {
							tuple_for_tokenizer.set(0, tuple_columns.get(i)
									.toString().trim().toUpperCase());
							tuple_for_tokenizer.set(1, listOfTokens.get(i)
									.toString().trim().toUpperCase());
							tuple.set(i * 2,
									protectStr.exec(tuple_for_tokenizer));
							tuple.set(i * 2 + 1, "Y");
						} else {
							tuple.set(i * 2, tuple_columns.get(i));
							tuple.set(i * 2 + 1, "N");
						}

					} else {
						tuple_for_tokenizer.set(0, tuple_columns.get(i)
								.toString().trim().toUpperCase());
						tuple_for_tokenizer.set(1, listOfTokens.get(i)
								.toString().trim().toUpperCase());
						tuple.set(i * 2, protectStr.exec(tuple_for_tokenizer));
						tuple.set(i * 2 + 1, "Y");
					}
				}
			}
			return tuple;

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try {

			Schema tupleSchema = new Schema();
			List<FieldSchema> listOfFields = getInputSchema().getFields();
			List<FieldSchema> listOfColumnSchema = listOfFields.get(0).schema
					.getFields();
			for (FieldSchema retval : listOfColumnSchema) {
				tupleSchema.add(new Schema.FieldSchema(retval.alias,
						DataType.CHARARRAY));
				tupleSchema.add(new Schema.FieldSchema("is_tokenized_"
						+ retval.alias, DataType.CHARARRAY));
			}
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), tupleSchema,
					DataType.TUPLE));

		} catch (Exception e) {
			return null;
		}

	}
}