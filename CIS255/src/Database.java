//package db;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//programed by kaya 
//cis 255 lab7 fall 2012
public class Database {
	public static final String URL_MYSQL5 = "jdbc:mysql://%s:3306/%s";
	public final static int MSSQL5 = 1;
	public final static int ORACLE = 2;
	public final static int MYSQL5 = 3;

	
    private String query;
    private int colCount;
	private final int rowCount=0;
    private int database_id;
	private int sqlCode;
	private String sqlMsg;
    
	private Connection conn;
    private String hostName;
    private String userName;
    private String password;
    private int driverId;

    private Statement statement;
	private CallableStatement cs;
	private PreparedStatement ps;

    private ResultSet rs;
	private ResultSetMetaData rsmd;

    public Database ()
    {
    	connect(MYSQL5, "localhost", "test", "root", null);
    }
    public Database (int db, String h, String d, String u, String p)
    {
    	connect (db, h,d,u,p);
    }
    public boolean connect ()
    {
    	return connect (driverId,hostName,"test", userName,password);
    	
    }
    public boolean connect (int db, String h, String d, String u, String p)
    {
		driverId = db;
		userName = u;
        password = p;

		try {
			switch (db) {
				case MYSQL5:
					hostName = String.format(URL_MYSQL5, h, d);
					DriverManager.registerDriver(new com.mysql.jdbc.Driver());
					break;

			}
			conn = DriverManager.getConnection (hostName, userName, password);

			/*
		case MSSQL5:
					hostName = String.format(URL_MSSQL5, h, d);
					DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
					break;
				case ORACLE:
					hostName = String.format(URL_ORACLE, h, d);
//					DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
					break;
				case ACCESS:
					hostName = String.format(URL_ACCESS, h, d);
					//DriverManager.registerDriver(new sun.jdbc.odbc.JdbcOdbcDriver());
					break;
				case ACCDB7:
					if (p==null || p.length() < 1)
						hostName = String.format(URL_ACCDB7, h, d);
					else
						hostName = String.format(URL_ACCDB7P, h, d, p);
					break;
				case INFORMIX:
					hostName = String.format(URL_INFOMX, h, d, d, u, p);
			}
			*/
//debug ("Connecting to: "+hostName);
			return true;
		} catch (SQLException sqle) {
			error("SQL-ERR: "+sqle.getMessage(), sqle.getErrorCode());
			return false;
		} catch (Exception e) {
			error("ERROR: "+e.getMessage(),0);
			return false;
		}
    }

    public boolean query(String sql)
    {
        colCount = 0;
        rs = null;
        query = sql;
        try {
            if (conn.isClosed()) {
                connect();
            }

            statement = conn.createStatement();

            rs = statement.executeQuery(sql);
//            debug(" DBConn.ExecQuery: " + sql);
 
			if (rs.next()) {
                rsmd = rs.getMetaData();
                colCount = rsmd.getColumnCount();
                return true;
            } else {
                rsmd = null;
                return false;
            }
        } catch (SQLException sqle) {
            error(sql+") SQL-Query-Error: " + sqle.getMessage(), sqle.getErrorCode());
            return false;
        } catch (Exception e) {
            error("ERROR:" + e.getMessage(), -1);
            return false;
        }
        //return false;
    }
    public String getColumnName (int i)
    {
    	try {
    	return rsmd.getColumnName(i);
    	} catch (Exception e){
    		return e.getMessage();
    	}
    }
    public int getColumnCount(){return colCount;}

    
	public String insert (ResultSetMetaData md, String alias)
	{
		StringBuilder sBuf = new StringBuilder ("INSERT INTO "+alias+"(");
		try {
			int colCount = md.getColumnCount();
			StringBuilder values = new StringBuilder(") VALUES (");
			for (int i = 0; i < colCount; ++i)
				if (i==0)
				{
					sBuf.append(md.getColumnName(i));
					values.append("?");
				} else {
					sBuf.append(",").append(md.getColumnName(i));
					values.append(",?");
				}
			sBuf.append(values).append(")");
		} catch (SQLException e) {
			System.err.println(e);
			return null;
		}
		return sBuf.toString();
	}
	public PreparedStatement prepareStatement (String sql) throws SQLException
	{
		return conn.prepareStatement(sql);
	}
	public void prepare(String sql, String... args) throws Exception
	{
		//System.out.printf ("Preparing: %s...", sql);
		try {
			if (conn==null || conn.isClosed()) connect();
			ps = conn.prepareStatement(sql);
//			preparedFlag = true;
			query=sql;
			System.out.println("DONE!!!");
			if (args==null) return;
			
			for(int i = 0; i < args.length; ++i)
				ps.setString(i+1, args[i]);

		} catch (SQLException sqle) {
			sqlMsg = sqle.getMessage();
			sqlCode = sqle.getErrorCode();
        	error("SQL-PRE-ERR: "+sqle.getMessage(), sqle.getErrorCode());
        	throw sqle; //new Exception(sqle.getMessage());
			//return;
		} catch (Exception e) {
			sqlMsg = e.getMessage();
			sqlCode = -1;
        	error("PRE-ERROR:"+e.getMessage(),0);
			throw e;
			//return;
		}
	}
	private int lastInsertID = 0;
	private final String lastInsertSQL = "SELECT LAST_INSERT_ID()";

	public int execute()
	{
	//System.out.println (query);
		try {
			switch (query.charAt(0))
			{
				case 'S':
				case 's':
					rs = ps.executeQuery();
					return (rs.next() ? 1 : 0);
				case 'i':
				case 'I':
	                if (ps.execute()) {
	                    ResultSet rst = getResultSet(lastInsertSQL);
	                    if (rst != null && rst.next()) {
	                        lastInsertID = rst.getInt(1);
	                        return lastInsertID;
	                    }
						return ps.getUpdateCount();
	                }
	                return -1;
				default:
                    return (ps.execute() ? ps.getUpdateCount() : -1);
			}
		} catch (SQLException sqle) {
				sqlMsg = sqle.getMessage();
				sqlCode = sqle.getErrorCode();
	        	error("SQL-EXEC-ERR: "+sqle.getMessage(), sqle.getErrorCode());
				sqle.printStackTrace();
	        	//throw sqle; //new Exception(sqle.getMessage());
				return -1;
		} catch (Exception e) {
				sqlMsg = e.getMessage();
				sqlCode = -1;
	        	error("ERROR:"+e.getMessage(),0);
				//throw e;
				return -1;
		}
	}
	private boolean preparedFlag;
	public int execute(String sql, Object... args)
	{
		try {
			if (sql != null) ps = conn.prepareStatement(sql);
			preparedFlag = true;
			for(int i = 0; i < args.length; ++i)
				if (args[i] instanceof Integer)
					ps.setInt(i+1, (Integer)args[i]);
				else
					ps.setString(i+1, (String)args[i]);

				switch (sql.charAt(0))
				{
					case 'S':
					case 's':
						rs = ps.executeQuery();
						return (rs.next() ? 1 : 0);
					case 'i':
					case 'I':
					    if (ps.execute()) {
					        ResultSet rst = getResultSet(lastInsertSQL);
					        if (rst.next()) {
					            lastInsertID = rst.getInt(1);
					            return lastInsertID;
					        }
					    }
					    return -1;
					default:
						return (ps.execute() ? 1 : -1);
				}
		} catch (SQLException sqle) {
			sqlMsg = sqle.getMessage();
			sqlCode = sqle.getErrorCode();
	       	error("SQL-ERR: "+sqle.getMessage(), sqle.getErrorCode());
	       	//throw sqle; //new Exception(sqle.getMessage());
			return -1;
		} catch (Exception e) {
			sqlMsg = e.getMessage();
			sqlCode = -1;
	       	error("ERROR:"+e.getMessage(),0);
			//throw e;
			return -1;
		}
	}
    public int insert (String table, Object... args)
    {
    	try {
    		StringBuilder sb = new StringBuilder(String.format("INSERT INTO %s VALUES (?", table));
    		for (int c = 1; c < args.length; ++c) sb.append(",?");
    		sb.append(")");
    		
    		ps = prepareStatement(sb.toString());
    		for (int i = 0; i < args.length; ++i)
        		ps.setString(i+1, String.valueOf(args[i]));

        	if (ps.executeUpdate() > 0) 
				return LastInsertID();
        	return -1;
    	} catch (Exception e) {
    		error (e.getMessage(), 0);
    		return -1;
    	}
    }
	public int getColCount(){return colCount;}
	public int LastInsertID()
	{
		try {
			ResultSet rst = getResultSet(lastInsertSQL);
	        if (rst != null && rst.next()) {
	            lastInsertID = rst.getInt(1);
	            return lastInsertID;
	        }
			return 0;
		} catch (Exception e) {
    		error (e.getMessage(), 0);
    		return -1;
    	}
    }

	private String procName;
	public boolean call(String sql, Object... args)
	{
		try {
			if (sql.indexOf("(") < 1)
			{
				sql += "(";
				for (int i = 0; i < args.length; ++i)
					sql += (i > 0 ? ",?" : "?");
				sql += ")";
			}

			cs = conn.prepareCall(String.format("{ call %s }", sql));
			preparedFlag = false;
			for(int i = 0; i < args.length; ++i)
				if (args[i] instanceof Integer)
					cs.setInt(i+1, (Integer)args[i]);
				else
					cs.setString(i+1, (String)args[i]);

			if (cs.execute()) {
           	   	rs = cs.getResultSet();
                rsmd = rs.getMetaData();
                colCount = rsmd.getColumnCount();

	           return rs.next();
           	}
           	return false;
		} catch (SQLException sqle) {
			sqlMsg = sqle.getMessage();
			sqlCode = sqle.getErrorCode();
        	error("SQL-ERR: "+sqle.getMessage(), sqle.getErrorCode());
        	//throw sqle; //new Exception(sqle.getMessage());
			return false;
		} catch (Exception e) {
			sqlMsg = e.getMessage();
			sqlCode = -1;
        	error("ERROR:"+e.getMessage(),0);
			//throw e;
			return false;
		}
	}
	public ResultSetMetaData getMetaData() {return rsmd;}
	public CallableStatement prepareCall(String pn) throws SQLException {return conn.prepareCall(String.format("{ call %s }", pn));}
	public int call ()
	{
		try {
			if (cs.execute()) {
			    rs = cs.getResultSet();
				return (rs.next() ? 2 : 1);
			}
			return 1;
		} catch (Exception e) {
			sqlMsg = e.getMessage();
			sqlCode = -1;
        	error("ERROR:"+e.getMessage(),0);
			//throw e;
			return -1;
		}
	}
    public ResultSet getResultSet() { return rs; }
    public ResultSet getResultSet (String sql)
    {
        try {
			if (conn.isClosed()) connect();

           	statement = conn.createStatement();
			//statement.close();
           	ResultSet rst = statement.executeQuery(sql);
			rs = rst;
           	return rst;
        } catch (SQLException sqle) {
        	error(sql+" Statment Caused the following SQL-Exception: "+sqle.getMessage(),sqle.getErrorCode());
            return null;
        } catch (Exception e) {
        	error(sql+" Statment Caused the following Exception: "+e.getMessage(),0);
            return null;
        }
    }

    public boolean getMoreResults() throws java.sql.SQLException
    {
    	if (cs.getMoreResults()){
    		rs = cs.getResultSet();
            rsmd = rs.getMetaData();
            colCount = rsmd.getColumnCount();
    		return rs.next();
    	}
    	return false;
    }
    public ResultSet getResultSet (String sql, Object... obj)
    {
        try {
			if (conn.isClosed()) connect();

           	PreparedStatement ps = conn.prepareStatement(sql);
			//statement.close();
			for (int i = 0; i < obj.length; ++i)
				ps.setString(i+1, (String)obj[i]);
           	return ps.executeQuery();
        } catch (SQLException sqle) {
        	error(sql+" Statment Caused the following SQL-Exception: "+sqle.getMessage(),sqle.getErrorCode());
            return null;
        } catch (Exception e) {
        	error(sql+" Statment Caused the following Exception: "+e.getMessage(),0);
            return null;
        }
    }

    public boolean getNext() throws Exception { return rs.next(); }
    public boolean fetch() {
        try {
            return rs.next();
        } catch (SQLException ex) {
            error(ex.getMessage(), ex.getErrorCode());
            return false;
        }
    }

    public void close()
    {
        try {
            if (rs != null) rs.close();
			if (conn != null) conn.close();
            conn=null;
        } catch (SQLException sqle) {
        	error("DBCONN Close Failed: "+sqle.getMessage(), sqle.getErrorCode());
            return;
        } finally {
			if (conn != null) conn = null;
        }
    }
    public String getString(String s) throws Exception { return rs.getString(s); }
    public String getString(int s) throws Exception { return rs.getString(s); }
    public int getInt(String s) throws Exception { return rs.getInt(s); }
    public int getInt(int s) throws Exception { return rs.getInt(s); }

    public void toFile(String fileName, boolean showColumns)
	{
        try {
			java.io.FileOutputStream fos = new java.io.FileOutputStream(new java.io.File(fileName));
			if (showColumns) fos.write(getColumns().getBytes());

            do {
				fos.write(getCommaDelimitedRow().getBytes());
            } while(rs.next());
            fos.close();
        } catch (SQLException sqle) {
        	error(sqle.getMessage(),sqle.getErrorCode());
            return;
        } catch (java.io.IOException ioex) {
        	error(ioex.getMessage(),4);
            return;
        }
	}
    public String getColumns ()
    {
		String retval = "";
    	try {
	       	rsmd = rs.getMetaData();
 	      	if (colCount == 0) {
	     		colCount = rsmd.getColumnCount();
   	            for (int i = 1; i < colCount+1; i++)
   	            {
   	            	int colType = rsmd.getColumnType(i);
              		retval+=rsmd.getColumnName(i);
              		if (i < colCount) retval+=",";
   	            }
	            retval += "\n";
 	      	}
       	} catch (SQLException sqle) {
        	close();
			return "SQL-ERROR: "+sqle.getMessage();
    	}
    	return retval;
    }

	public String getCommaDelimitedRow()
	{
        try {
	       	if (rsmd == null) rsmd = rs.getMetaData();

     		colCount = rsmd.getColumnCount();

           	String retval="";
           	int oldColType = rsmd.getColumnType(1);
            for (int i = 1; i < colCount +1; ++i) {
           		int colType = rsmd.getColumnType(i);
                String v = rs.getString(i);
    	        if (v == null) v = " ";

    	        if (colType == 93 && v.length() > 10)
    	        	retval += "\" "+v.substring(5,7)+"/"+
						v.substring(8,10)+"/"+v.substring(0,4);
				else if (v.length() > 0 && (v.charAt(0) >= '0' && v.charAt(0) <= '9'))
                    retval += "\" "+v;
				else
                    retval += "\""+v;

           		if (i < colCount)
           			retval+="\",";
           		else
	       			retval+="\"";
            }
            retval += "\n";
			return retval;
        } catch (SQLException sqle) {
        	error(sqle.getMessage(),sqle.getErrorCode());
            return "SQL-ERROR: "+sqle.getMessage();
        }
	}


	public void toFile(String fileName, String deli, boolean showColumns)
	{
        try {
			java.io.FileOutputStream fos = new java.io.FileOutputStream(new java.io.File(fileName));
			if (showColumns)
			{
				for (int i = 1; i <= colCount; ++i)
					fos.write((rsmd.getColumnName(i)+deli).getBytes());
				fos.write ((byte)'\n');
			}
			String cap = (deli.charAt(0)==',' ? "\"" : "");
            do {
				for (int i = 1; i <= colCount; ++i)
					fos.write(String.format("%s%s%s%s", cap,ifnull(rs.getString(i), "").trim(),cap,deli).getBytes());
				fos.write ((byte)'\n');
            } while(rs.next());
            fos.close();
        } catch (SQLException sqle) {
        	error(sqle.getMessage(),sqle.getErrorCode());
            return;
        } catch (java.io.IOException ioex) {
        	error(ioex.getMessage(),4);
            return;
        }
	}
	public String ifnull(String val, String alt){return (val==null?alt:val);}

	public void toFile(String f1, String f2, String deli, boolean showColumns)
	{
        try {
			java.io.FileOutputStream fos1 = new java.io.FileOutputStream(new java.io.File(f1));
			java.io.FileOutputStream fos2 = new java.io.FileOutputStream(new java.io.File(f2));
			if (showColumns)
			{
				for (int i = 1; i <= colCount; ++i){
					byte[] h = getColumns().getBytes();
					fos1.write(h);
					fos2.write(h);				
				}
				fos1.write ((byte)'\n');
				fos2.write ((byte)'\n');
			}
			String cap = (deli.charAt(0)==',' ? "\"" : "");
            do {
				for (int i = 1; i <= colCount; ++i){
					byte[] data = 	String.format("%s%s%s%s", cap,ifnull(rs.getString(i), "").trim(),cap,deli).getBytes();

					fos1.write(data);
					fos2.write(data);
				}
				fos1.write ((byte)'\n');
				fos2.write ((byte)'\n');
            } while(rs.next());
            fos1.close();
			fos2.close();
        } catch (SQLException sqle) {
        	error(sqle.getMessage(),sqle.getErrorCode());
            return;
        } catch (java.io.IOException ioex) {
        	error(ioex.getMessage(),4);
            return;
        }
	}

	public static String[] file(String fileName)
	{
		BufferedReader br = null;

        try {
			br = new BufferedReader(new FileReader(fileName));
			String thisLine = br.readLine();
			java.util.Vector<String> vec = new java.util.Vector<String>();
			do {
				vec.add(thisLine);
			}while ((thisLine  = br.readLine()) != null) ;
            br.close();
			return vec.toArray(new String[vec.size()]);
        } catch (java.io.IOException ioex) {
        	//error(ioex.getMessage(),4);
            return null;
        }
	}

	public int importFile (String Insert, String fileName, String deli, int skip, int maxCols)
	{
		int rows=0;
		BufferedReader br = null;
        try {
			prepare(Insert);
			br = new BufferedReader(new FileReader(fileName));

			String thisLine = null;
			for (int i=0; i < skip; ++i) thisLine = br.readLine();
			String[]cols = Insert.split(",");
			thisLine = br.readLine();
			do {
				String data[] = thisLine.split(deli);
				int len = data.length;
				if (len > maxCols) len = maxCols;
				for (int i=0; i < len; ++i) {
					System.out.printf ("%s = %s \n", cols[i], data[i]);
					if (cols[i].indexOf("Date") > 0)
						setDate(i+1, data[i]);
					else
						setObject (i+1, data[i].replaceAll("\"", ""));
				}
				execute();

				++rows;
			}while ((thisLine = br.readLine()) != null) ;
            br.close();
        } catch (SQLException sqle) {
        	error(sqle.getMessage(),sqle.getErrorCode());
            return -1;
        } catch (java.io.IOException ioex) {
        	error(ioex.getMessage(),4);
            return -1;
        } catch (Exception ex) {
        	error(ex.getMessage(),4);
            return -1;
        }
		return rows;
	}
	public void setDate (int n, java.sql.Date v)  throws Exception
	{
		try {
			if (preparedFlag)
				ps.setDate(n, v);
			else
				cs.setDate(n, v);
		} catch (Exception e) {
			System.out.printf ("Date String: %s failed to convert.\n", v);
			if (preparedFlag)
				ps.setDate(n, null);
			else
				cs.setDate(n, null);
		}

	}
	public void setDate (int n, String v) throws Exception
	{
		try {
			if (preparedFlag)
				ps.setInt(n,intDate(v));
			else
				cs.setInt(n, intDate(v));
		} catch (Exception e) {
			System.out.printf ("Date String: %s failed to convert.\n", v);
			if (preparedFlag)
				ps.setString(n, v);
			else
				cs.setString(n, v);
		}
	}
	public static int intDate () {return intDate(null);}
	private static java.util.Calendar cal = java.util.Calendar.getInstance();
	public static int intDate (String dt) {
		if (dt==null) {
			cal.setTime(new java.util.Date());
			int y = cal.get(java.util.Calendar.YEAR);
			if (y < 1900) y+=1900;
			return y*10000 
				+ (cal.get(java.util.Calendar.MONTH) +1) * 100 
				+ cal.get(java.util.Calendar.DATE);
		}
		//dt = new java.util.Date().toString();
		//Tue Nov 15 01:24:40 PST 2011 
		if (dt.length() > 10)
		{
			//System.out.println ("Date: "+dt);
			String t[] = dt.split(" ");
			int y = Integer.parseInt(t[5]) * 10000;
			int m = monthToInt(t[1]) *100 ;
			int d = Integer.parseInt(t[2]);
			return y + m + d;
		}
		int index = dt.indexOf ("/");
		if (index > 0) // MM/DD/YY
		{
			String t[] = dt.split("/");
			int y = Integer.parseInt(t[2]) * 10000;
			int m = Integer.parseInt(t[0]) * 100;
			int d = Integer.parseInt(t[1]);
			return y + m + d;
		}
		index = dt.indexOf ("-");
		if (index > 0) // [CC]YY-MM-DD
		{
			int d = Integer.parseInt(dt.replaceAll("-", ""));
			if (d < 19000000) return d + 20000000;
			return d;
		}
		return 0;
	}
	private static int monthToInt (String mon)
	{
		switch (mon.charAt(0))
		{
			case 'J':
				if (mon.charAt(1) == 'a') return 1;
				if (mon.charAt(2) == 'n') return 6;
				return 7;
			case 'F': return 2;
			case 'M':
				if (mon.charAt(2) == 'r') return 3;
				return 5;
			case 'A':
				if (mon.charAt(2) == 'r') return 4;
				return 8;
			case 'S': return 9;
			case 'O': return 10;
			case 'N': return 11;
			case 'D':
			default: return 12;
		}
	}


	public void setObject (int i, Object obj)
	{
		try {
			if (obj==null) 
			{
				ps.setString(i, null);
				return;
			}
			String val = obj.toString();
			int iVal = parseInt(val, 0);
			if (iVal > 0) {
				double d = Double.parseDouble(val);
				if (d == iVal)
					ps.setInt(i, iVal);
				else
					ps.setDouble(i, d);
				return;
			}
			ps.setString(i, val);
		} catch (Exception e) {
        	error(" Statment Caused the following Exception: "+e.getMessage(),0);
            return ;
        }
		return;
	}
	public static int parseInt(String val, int alt)
	{
		try {
			return Integer.parseInt(val);
		} catch (Exception e){
			return alt;
		}
	}
	public static void error (String msg, Object...args)
	{
		System.err.printf(msg, args);
	}
}
