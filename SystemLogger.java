import java.text.SimpleDateFormat;
import java.util.Date;

public class SystemLogger {
	private Class<?> caller;
	private String callerName;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
	
	//Getter method
	public static SystemLogger getSystemLogger(Class<?> caller) {
		return new SystemLogger(caller);
	}
	
	//Constructor method
	public SystemLogger(Class<?> caller){
		this.caller = caller;
		this.callerName = caller.getCanonicalName();
	}
	
	//For info messages
	public void info(Object message){
		Date date = new Date();
		System.out.println(dateFormat.format(date) + " | " + callerName + " | Thread " + Thread.currentThread().getId() + " | " + "INFO | " + String.valueOf(message));
	}
	
	//For error messages
	public void err(Object message){
		Date date = new Date();
		System.out.println(dateFormat.format(date) + " | " + callerName + " | Thread " + Thread.currentThread().getId() + " | " + "ERROR | " + String.valueOf(message));
		
	}
}
