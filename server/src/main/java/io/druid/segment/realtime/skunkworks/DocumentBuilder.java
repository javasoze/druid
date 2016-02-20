package io.druid.segment.realtime.skunkworks;

import io.druid.data.input.InputRow;

import org.apache.lucene.document.Document;

/**
 * Converts an InputRow to a Lucene Document
 *
 */
public interface DocumentBuilder {
  Document buildDocument(InputRow row);
}
