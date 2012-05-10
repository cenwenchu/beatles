package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

/**
 * EntryKey.java
 * @author yunzhan.jtq
 * 
 * @since 2012-5-3 下午10:42:43
 */
public class EntryKey implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 7467745117713840302L;

    private String value;

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }
    
    @Override
    public int hashCode() {
        return value.hashCode();
    }
    
    @Override
    public boolean equals(Object e) {
        if(e == null)
            return false;
        if(!(e instanceof EntryKey))
            return false;
        EntryKey e2 = (EntryKey)e;
        if(this.value == null && e2.value != null)
            return false;
        if(this.value == null && e2.value == null)
            return true;
        return this.value.equals(e2.value);
    }
}
