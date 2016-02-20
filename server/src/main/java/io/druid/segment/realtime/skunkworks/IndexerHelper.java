package io.druid.segment.realtime.skunkworks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

import com.google.common.base.Charsets;

public class IndexerHelper {
	public final static FieldType STORE_FIELD_TYPE = new FieldType();
	public final static FieldType TEXT_FIELD_TYPE = new FieldType();
	public final static FieldType STRING_META_FIELD_TYPE = new FieldType();
	public final static FieldType STRING_TEXT_FIELD_TYPE = new FieldType();
	public final static FieldType META_FIELD_TYPE = new FieldType();

	static {
		STORE_FIELD_TYPE.setStored(true);
		STORE_FIELD_TYPE.setTokenized(false);
		STORE_FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
		STORE_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
		STORE_FIELD_TYPE.setStoreTermVectors(false);
		STORE_FIELD_TYPE.freeze();

		TEXT_FIELD_TYPE.setStored(true);
		TEXT_FIELD_TYPE.setOmitNorms(true);
		TEXT_FIELD_TYPE.setTokenized(true);
		TEXT_FIELD_TYPE
				.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		TEXT_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
		TEXT_FIELD_TYPE.setStoreTermVectors(false);
		TEXT_FIELD_TYPE.freeze();

		STRING_TEXT_FIELD_TYPE.setStored(true);
		STRING_TEXT_FIELD_TYPE.setOmitNorms(true);
		STRING_TEXT_FIELD_TYPE.setTokenized(false);
		STRING_TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
		STRING_TEXT_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
		STRING_TEXT_FIELD_TYPE.setStoreTermVectors(false);
		STRING_TEXT_FIELD_TYPE.freeze();

		STRING_META_FIELD_TYPE.setStored(false);
		STRING_META_FIELD_TYPE.setOmitNorms(true);
		STRING_META_FIELD_TYPE.setTokenized(false);
		STRING_META_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
		STRING_META_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
		STRING_META_FIELD_TYPE.setStoreTermVectors(false);
		STRING_META_FIELD_TYPE.freeze();
	}

	public static void textField(Document doc, String name, String text,
			boolean tokenized) {
		/*
		 * RapidField field = new RapidField(name); field.value = text;
		 * field.isTokenized = tokenized; field.isSearchable = true;
		 * field.isMeta = false; return field;
		 */
		doc.add(new Field(name, text == null ? "" : text,
				tokenized ? TEXT_FIELD_TYPE : STRING_TEXT_FIELD_TYPE));
	}

	public static void metaField(Document doc, String name, String val,
			boolean multi) {
		/*
		 * RapidField field = new RapidField(name); field.value = val;
		 * field.isTokenized = false; field.isSearchable = searchable;
		 * field.isMeta = true; field.isNumber = false; return field;
		 */
		doc.add(new Field(name, val == null ? "" : val, STRING_META_FIELD_TYPE));
		if (multi) {
			doc.add(new SortedSetDocValuesField(name, new BytesRef(
					(val == null ? "" : val).getBytes(Charsets.UTF_8))));
		} else {
			doc.add(new SortedDocValuesField(name, new BytesRef(
					(val == null ? "" : val).getBytes(Charsets.UTF_8))));
		}

	}
}
