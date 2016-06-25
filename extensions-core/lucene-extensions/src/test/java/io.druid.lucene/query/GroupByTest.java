package io.druid.lucene.query;

import com.amazonaws.util.json.Jackson;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByTest {
    @Test
    public void test() throws ParseException, IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        Directory d = FSDirectory.open(Paths.get("data"));
//        IndexWriter writer = new IndexWriter(d, iwc);
//        Document doc = new Document();
//        doc.add(new TextField("name", "yaotc", Field.Store.NO));
//        writer.addDocument(doc);
//        writer.close();
        IndexReader reader = DirectoryReader.open(d);
        List<LeafReaderContext> leaves = reader.leaves();
        for (LeafReaderContext context: leaves) {
            IndexSearcher searcher = new IndexSearcher(context.reader());
//            searcher.search(new TermQuery(new Term("", "")), new DocSetCollector(10, 100));
        }
    }

    @Test
    public void test1() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("timestamp", new DateTime(new Date().getTime()).toString());
        map.put("name", "b");
        map.put("value", 1222);
        System.out.println(Jackson.toJsonString(map));
    }
}
