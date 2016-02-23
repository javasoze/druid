package io.druid.segment.realtime.skunkworks;

import io.druid.data.input.InputRow;

import java.util.Set;

import org.apache.lucene.document.Document;

/**
 * Converts an InputRow to a Lucene Document
 *
 */
public interface DocumentBuilder
{
  public static final String SYSTEM_TIME_FIELD = "_time";
  Document buildDocument(InputRow row);
  Set<String> searchableFields();
  Set<String> metaFields();
}
