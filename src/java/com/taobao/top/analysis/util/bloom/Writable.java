/**
 * 
 */
package com.taobao.top.analysis.util.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-3-16
 *
 */
public interface Writable {

	void write(DataOutput out) throws IOException;
	
	void readFields(DataInput in) throws IOException;

}
