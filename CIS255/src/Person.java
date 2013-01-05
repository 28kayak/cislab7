import java.sql.ResultSet;
import java.sql.SQLException;
//person.java with sql connection
//programed by kaya 
//cis 255 lab7 fall 2012
public class Person 
{
	private static String username = "root";
	private static String password = "0246kaya";
	private static String host = "localhost";
	private static String database = "test";
	
	private String lastName;
	private String firstName;
	private String emailAddress;
	private String phoneNumber;
	private int id;
	
	public Person()
	{
		id = getId();
	}
	public Person(String fn, String ln, String e, String p)
	{
		firstName = fn;
		lastName = ln;
		emailAddress = e;
		phoneNumber = p;
		
		save();
		
	}//Person1
	public Person(int idNum, String fn, String ln, String e, String p)
	{
		id = idNum;
		firstName = fn;
		lastName =ln;
		emailAddress = e;
		phoneNumber = p;
	}//previous one with id
	
	public void setLastName(String ln)
	{
		lastName = ln;
	}
	public void setFirstName(String fn)
	{
		firstName = fn;
	}
	public void setEmailAddress(String e)
	{
		emailAddress = e;
	}
	public void setPhoneNumber(String p)
	{
		phoneNumber = p;
	}
	public void setId(int idNum)
	{
		id = idNum;
	}
	//get methods 
	public String getLastName()
	{
		return lastName;
	}
	public String getFirstName()
	{
		return firstName;
	}
	public String getEmailAddress()
	{
		return emailAddress;
	}
	public String getPhoneNumber()
	{
		return phoneNumber;
	}
	public int getId()
	{
		return id;
	}
	public boolean save()
	{	
		Database db = new Database(Database.MYSQL5, host, database, username, password);
		//make a connection between java and database.
		//db.query("SELECT * FROM Person");
		try
		{
			db.execute("INSERT into Person(firstName, lastName, email,phone_nr) values( ?, ?, ?, ?)",
					getFirstName(), getLastName(),getEmailAddress(),getPhoneNumber());//translate java to SQL and execute without returning data
		
			
			/*while(rs.next())
			{	
				String enteredName = "kaya ota";
				
				String name = rs.getString(1)+" " + rs.getString(2);
				System.out.println(name);
				//System.out.printf("id: %d  firstname: %s   lastname : %s\n", 
    				//	rs1.getInt(1), rs1.getString(2), rs1.getString(3));
				if(enteredName.equalsIgnoreCase(name))
				{
					System.out.printf("here is %s %s\n", 
	    					rs.getString(1), rs.getString(2));
	    				
				}//if
			}//while
		*/
			
			return true;
		}//try
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return false;
		}
		finally 
		{
			db.close();
		}
		
	}//save()
	
	public static boolean read(int idnum)
	{
		boolean result = false;
		
		//System.out.println("main");
		Database db = new Database(Database.MYSQL5, host, database, username, password);
		//Database db = new Database();
		//db.query("SELECT * FROM Person");
		try
		{
			ResultSet rs = db.getResultSet("SELECT * FROM Person where id = ?", String.valueOf(idnum));
			//SELECT statement will return some data so that use getResultSet()
			while(rs.next())
			{	
				System.out.printf("%d %s %s %s %s",rs.getInt(1), rs.getString(2), rs.getString(3),rs.getString(4),rs.getString(5));
				
				
				//System.out.printf("id: %d  firstname: %s   lastname : %s\n", 
					//rs.getInt(1), rs.getString(2), rs.getString(3));
			}
			 result = true;
		}
		
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
			 result = false;
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			 result = false;
			
		}
		finally 
		{
			db.close();
			//return result;
		}
		return result;
		
	}//read
	
		
		public static void listAll()
		{
			//System.out.println("main");
			Database db = new Database(Database.MYSQL5, host, database, username, password);
			//Database db = new Database();
			//db.query("SELECT * FROM Person");
			try
			{
				ResultSet rs = db.getResultSet("SELECT * FROM Person");
				while(rs.next())
				{	
					System.out.printf("%d %s %s %s %s\n",rs.getInt(1), rs.getString(2), rs.getString(3),rs.getString(4),rs.getString(5));
					
					
					//System.out.printf("id: %d  firstname: %s   lastname : %s\n", 
						//rs.getInt(1), rs.getString(2), rs.getString(3));
				}
		
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
			finally
			{
				db.close();
			}
		}//listAll()
		
		public static void listAll(String name)
		{
			Database db = new Database(Database.MYSQL5, host, database, username, password);
			try
			{
				ResultSet rs = db.getResultSet("SELECT * FROM Person WHERE firstName = ? or lastname = ?", name, name);
				while(rs.next())
				{	
					System.out.printf("%d %s %s %s %s\n",rs.getInt(1), rs.getString(2), rs.getString(3),rs.getString(4),rs.getString(5));
					//resultSet　instanceのcolumnの数
					
					
					//System.out.printf("id: %d  firstname: %s   lastname : %s\n", 
						//rs.getInt(1), rs.getString(2), rs.getString(3));
				}
		
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
			finally
			{
				db.close();
			}
			
		}//listAll(String name)
		
		public boolean delete()
		{
			boolean result = false;
			Database db = new Database(Database.MYSQL5, host, database, username, password);
			try
			{
				
				int idnum = this.getId();
				db.execute("DELETE FROM Person WHERE id = ?",idnum);//execute()is used when no data will be returned.
				
				//db.fetch()//一行進めたり前に戻したり。
				result = true;
				
			}
			
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
				result = false;
			}
				db.close();
				return result;
			
			
		}//delete
		/*
		 public boolean delete()
		    {
		        boolean result = false;
					
		        Database db = new Database(Database.MYSQL5, host, database, username, password);
			int idnum = this.getId();
		        
		        System.out.printf("You deleted a Student with an ID#: %d ", this.getId() );
		        read(idnum);
		        
		        try
		        {    
		            db.execute("DELETE FROM Person.person WHERE id = ?",idnum);
		            System.out.println("\nHere is your updated database:");
		            listAll();
		            result = true;
			}
					
			catch(Exception ex)
			{
		            System.out.println(ex.getMessage());
		                result = false;
			}
		            db.close();
		            return result;		
			}
		
		*/
	


	
	
	

	
	
}//class
