package io.druid.lucene.query;

import java.util.List;

/**
 */
public interface DimensionSelector<T> {
    /**
     * Gets all values for the row inside of an IntBuffer.  I.e. one possible implementation could be
     *
     * return IntBuffer.wrap(lookupExpansion(get());
     *
     * @return all values for the row as an IntBuffer
     */
    public List<T> getRow(int doc);

    /**
     * The Name is the String name of the actual field.  It is assumed that storage layers convert names
     * into id values which can then be used to get the string value.  For example
     *
     * A,B
     * A
     * A,B
     * B
     *
     * getRow() would return
     *
     * getRow(0) =&gt; [0 1]
     * getRow(1) =&gt; [0]
     * getRow(2) =&gt; [0 1]
     * getRow(3) =&gt; [1]
     *
     * and then lookupName would return:
     *
     * lookupName(0) =&gt; A
     * lookupName(1) =&gt; B
     *
     * @param id id to lookup the field name for
     * @return the field name for the given id
     */
    public String lookupName(T id);

    /**
     * The ID is the int id value of the field.
     *
     * @param name field name to look up the id for
     * @return the id for the given field name
     */
    public T lookupId(String name);
}
