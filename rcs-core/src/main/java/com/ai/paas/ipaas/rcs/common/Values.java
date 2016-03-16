package com.ai.paas.ipaas.rcs.common;

/**
 * 类似storm的Values
 */ 

import java.util.ArrayList;

/**
 * A convenience class for making tuple values using new Values("field1", 2, 3) syntax.
 */
public class Values extends ArrayList<Object>
{
	/**
	 * default constructor
	 */
    public Values() {
        
    }
    
    /**
     * constructor using object lists 
     * @param vals
     */
    public Values(Object... vals) 
    {
        super(vals.length);
        
        for(Object o: vals) 
        {
            add(o);
        }
    }
}

