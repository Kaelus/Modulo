package strata.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ObjectCopy {

	public static Object copy(final Serializable obj) throws IOException, ClassNotFoundException {
	    ObjectOutputStream out = null;
	    ObjectInputStream in = null;
	    Object copy = null;
	
	    try {
	      // write the object
	      ByteArrayOutputStream baos = new ByteArrayOutputStream();
	      out = new ObjectOutputStream(baos);
	      out.writeObject(obj);
	      out.flush();
	
	      // read in the copy
	      byte data[] = baos.toByteArray();
	      ByteArrayInputStream bais = new ByteArrayInputStream(data);
	      in = new ObjectInputStream(bais);
	      copy = in.readObject();
	    } finally {
	      out.close();
	      in.close();
	    }
	
	    return copy;
	  }
	
}
