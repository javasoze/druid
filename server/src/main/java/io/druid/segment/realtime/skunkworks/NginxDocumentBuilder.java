package io.druid.segment.realtime.skunkworks;

import java.util.List;

import io.druid.data.input.InputRow;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.joda.time.DateTime;

public class NginxDocumentBuilder implements DocumentBuilder
{

  private static String extractVal(InputRow row, String dim)
  {
    List<String> valList = row.getDimension(dim);
    if (valList != null && !valList.isEmpty())
    {
      return valList.get(0);
    } else
    {
      return null;
    }
  }

  @Override
  public Document buildDocument(InputRow row)
  {
    Document doc = new Document();
    DateTime time = row.getTimestamp();

    // only store seconds
    long timeInSec = time.getMillis() / 1000;
    doc.add(new NumericDocValuesField(SYSTEM_TIME_FIELD, timeInSec));
    doc.add(new LongField(SYSTEM_TIME_FIELD, timeInSec, Store.NO));

    String host = extractVal(row, "host");
    if (host != null)
    {
      IndexerHelper.metaField(doc, "host", host, false);
    }

    String request = extractVal(row, "request");
    if (request != null)
    {
      IndexerHelper.textField(doc, "request", request, true);
    }

    String response = extractVal(row, "response");
    if (response != null)
    {
      IndexerHelper.metaField(doc, "response", response, false);
    }

    String agent = extractVal(row, "agent");
    if (agent != null)
    {
      IndexerHelper.textField(doc, "agent", agent, true);
    }

    return doc;
  }

}
