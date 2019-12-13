package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    private Type[] types;
    private String[] attrNames;
    private int numfields;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        TupleDesc newTupleDesc = new TupleDesc(td1.numfields + td2.numfields);
        for (int i = 0; i < td1.numfields; i += 1) {
            newTupleDesc.setTypes(i, td1.getType(i));
            newTupleDesc.setAttrNames(i, td1.getFieldName(i));
        }
        for (int i = 0; i < td2.numfields; i += 1) {
            newTupleDesc.setTypes(i+td1.numfields, td2.getType(i));
            newTupleDesc.setAttrNames(i+td1.numfields, td2.getFieldName(i));
        }
        return newTupleDesc;
    }

    public void setTypes(int i, Type type) {
        types[i] = type;
    }

    public void setAttrNames(int i, String name) {
        attrNames[i] = name;
    }

    public TupleDesc(int i) {
        this.numfields = i;
        types = new Type[i];
        attrNames = new String[i];
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert typeAr.length > 0;
        this.types = typeAr;
        this.attrNames = fieldAr;
        this.numfields = typeAr.length;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        assert typeAr.length > 0;
        this.numfields = typeAr.length;
        this.types = typeAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return numfields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i > numfields-1) {
            throw new NoSuchElementException();
        }
        if (attrNames == null || attrNames.length < (i+1)) {
            return null;
        }
        return attrNames[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        if (attrNames == null) throw new NoSuchElementException();
        for (int i = 0; i < attrNames.length; i += 1) {
            if (attrNames[i].equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        if (i < 0 || i > numfields-1) throw new NoSuchElementException();
        return types[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int acc = 0;
        for (Type type : types) {
            acc += type.getLen();
        }
        return acc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc other = (TupleDesc) o;

        if (other.numFields() != numfields) return false;
        for (int i = 0; i < numfields; i += 1) {
            if (other.getType(i) != types[i]) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < numfields; i += 1) {
            String item = "" + types[i];
            if (attrNames == null || attrNames[i] == null) {
                sb.append(item).append("()");
            } else {
                sb.append(item).append("(").append(attrNames[i]).append(")");
            }
            if (i < numfields-1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
}
