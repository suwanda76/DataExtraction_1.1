package myExImDB;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.DefaultListModel;
import javax.swing.JComboBox;
import javax.swing.JList;

import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListModel;
import javax.swing.table.DefaultTableModel;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgresql.jdbc.AbstractBlobClob;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class myDBConnection {
	private JComboBox cboDB;

	
	public void getConnection(JComboBox cboDb) {
		String user="sa";
		String pass ="administrator";	
		Connection conn= null;
		try {
		//jdbc:sqlserver://localhost:1433;databaseName=Test"
			String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
			//String dbURL = "jdbc:sqlserver://SUWANDA/SQLEXPRESS:1433;databasename=MyNewDB;user="+user+ ";password="+ pass;
			//Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(dbURL);	
		
			  DatabaseMetaData meta = conn.getMetaData(); 
			  ResultSet res = meta.getCatalogs(); 
			  System.out.println("List of databases: ");
			  while (res.next()) {
				  System.out.println("   " +res.getString("TABLE_CAT"));
				  cboDb.addItem(res.getString("TABLE_CAT"));
			  }
		      
			if (conn != null) {
			    System.out.println("Connected");
			}
			
		} catch (SQLException ex) {
            ex.printStackTrace();
        } 
		finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	  }
   
	public void displayAvailableTables(JList tblList, String selectedDB){
		Connection conn= null;
		String user="sa";
		String pass ="administrator";	
		try {
			String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
		     conn = DriverManager.getConnection(dbURL);	
		     Statement stmt = conn.createStatement();
		  		     
		     String query ="";
		     query = "Use "+ selectedDB ;
		     query = query + " Select TABLE_SCHEMA +'.'+ TABLE_NAME ";
		     query = query + " From INFORMATION_SCHEMA.TABLES ";		 
		     query = query + " Where TABLE_CATALOG ='"+ selectedDB +"' ";
		     query = query + " Order by TABLE_NAME ASC";
		     ResultSet rs = stmt.executeQuery(query);
			// int arrTblIdx = 0; 
		     System.out.println(query);
			 DefaultListModel dlm = new DefaultListModel();
			 ListModel lm  = tblList.getModel();
			 
			 while (rs.next()) {
			    String x = rs.getString(1);
			    dlm.addElement(x);
			    // tblArr [arrTblIdx]= x;			     
			    System.out.println(x);
			    //arrTblIdx++;
			    tblList.setModel(dlm);
		     }
			
			  
			if (conn != null) {
			    System.out.println("Connected");
			}
			
		} catch (SQLException ex) {
            ex.printStackTrace();
        } 
		finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
	public void displayAllFields(JList lstTableFields, String selectedDB, Vector selectedTables) {
		Connection conn= null;
		String user="sa";
		String pass ="administrator";	
		try {
			String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
		     conn = DriverManager.getConnection(dbURL);	
		     Statement stmt = conn.createStatement();
		     
		     ///FOR INNER JOIN MUST MODIFY THIS CODE
		     String selectedTableValue = (String) selectedTables.get(0);
		     System.out.println("VALUEEEE OF selected Table : " + selectedTableValue);
		     String[] parts = selectedTableValue.split("\\.");
		     String tblSchema = parts[0];
		     String tblName = parts[1]; 
		     
		     String query ="";
		     query = "Use "+selectedDB ;
		     query = query + " Select COLUMN_NAME ";
		     query = query + " From INFORMATION_SCHEMA.COLUMNS ";		 
		     query = query + " Where TABLE_NAME ='"+ tblName +"' ";
		     query = query + " and TABLE_SCHEMA ='"+ tblSchema +"' ";
		     query = query + " Order by COLUMN_NAME ASC";
		     System.out.println(query);
		     ResultSet rs = stmt.executeQuery(query);
		
		     System.out.println(query);
			 DefaultListModel dlm = new DefaultListModel();
			// ListModel lm  = lstTableFields.getModel();
			 
			 while (rs.next()) {
			    String x = rs.getString(1);
			    dlm.addElement(x);   	     
			    System.out.println(x);
			    lstTableFields.setModel(dlm);
		     }
			  
			if (conn != null) {
			    System.out.println("Connected");
			}
			
		} catch (SQLException ex) {
            ex.printStackTrace();
        } 
		finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
	public void getDisplayCSV(JTextArea txtArea, String selectedDB, Vector selectedTables, Vector selectedFields) {
		Connection conn= null;
		String user="sa";
		String pass ="administrator";	
		try {
			String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
		     conn = DriverManager.getConnection(dbURL);	
		     Statement stmt = conn.createStatement();
		     
		     String [] arrSelectedFields =  (String[]) selectedFields.toArray(new String[selectedFields.size()] );
		     String joinedSelectedFields = String.join(",", arrSelectedFields);
		     
		     ///This is for the Header ADD the New line
		     txtArea.setText(joinedSelectedFields + "\n" );
		     
		     String selectedTableValue = (String) selectedTables.get(0);
		     String query ="";
		     query = "Use "+selectedDB ;
		     query = query + " Select top (10) "+ joinedSelectedFields;
		     query = query + " From "+ selectedTableValue;		 
				
		     System.out.println(query);
		     ResultSet rs = stmt.executeQuery(query);
		
			 int numOfColHeader = selectedFields.size();
			 if (numOfColHeader ==0) {
				 numOfColHeader = 1;
			 }else {
				 numOfColHeader = selectedFields.size() + 1;
			 }
			 //int countColHeader = 0;
			 String colValue ="";
			 while (rs.next()) {
				 for (int i = 1; i < numOfColHeader; i++) {
					String val = (String) rs.getString(i);	
			    	colValue = colValue + val + ",";		
				 }
			    	colValue = colValue+ "\n";			 	
		     }

			 //SEt all the values
			 txtArea.setText(txtArea.getText() + colValue) ;
			if (conn != null) {
			    System.out.println("Connected");
			}
			
		} catch (SQLException ex) {
            ex.printStackTrace();
        } 
		finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}	
	
	
	 public void getDisplayJSON(JTextArea txtArea, String selectedDB, Vector selectedTables, Vector selectedFields) {
		 	Connection conn= null;
			String user="sa";
			String pass ="administrator";	
			
			String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
			     try {
					conn = DriverManager.getConnection(dbURL);
				
			     Statement stmt = conn.createStatement();
			     
			     String [] arrSelectedFields =  (String[]) selectedFields.toArray(new String[selectedFields.size()] );
			     String joinedSelectedFields = String.join(",", arrSelectedFields);
			     
			   
			     String selectedTableValue = (String) selectedTables.get(0);
			     String query ="";
			     query = "Use "+selectedDB ;
			     query = query + " Select top (10)"+ joinedSelectedFields;
			     query = query + " From "+ selectedTableValue;		 
				
			     System.out.println(query);
			     ResultSet rs = stmt.executeQuery(query);
			
		 		ResultSetMetaData rsmd = rs.getMetaData();
			   	JSONArray json = new JSONArray();
			    
				String jsonString = "";			   	
			    while(rs.next()) {
			    	JSONObject obj= new JSONObject();
				    int numColumns = rsmd.getColumnCount();
				      ///////////////////LOOP////
				      for (int i = 1; i <= numColumns; i++) {
				          System.out.println("Num of Col :" + numColumns);
					      String column_name = rsmd.getColumnName(i);
					      System.out.println("Column Name : " + column_name);

				    	  switch( rsmd.getColumnType( i ) ) {
					      case java.sql.Types.ARRAY:
					        obj.put(column_name, rs.getArray(column_name));     break;
					      case java.sql.Types.BIGINT:
					        obj.put(column_name, rs.getInt(column_name));       break;
					      case java.sql.Types.BOOLEAN:
					        obj.put(column_name, rs.getBoolean(column_name));   break;
					      case java.sql.Types.BLOB:
					    	{
						    	System.out.println("--------------BLOB : "+ column_name);
						        obj.put(column_name, rs.getBlob(column_name));     
						        break;
					    	}
					      case java.sql.Types.DOUBLE:
					        obj.put(column_name, rs.getDouble(column_name));    break;
					      case java.sql.Types.FLOAT:
					        obj.put(column_name, rs.getFloat(column_name));     break;
					      case java.sql.Types.INTEGER:
					        obj.put(column_name, rs.getInt(column_name));       break;
					      case java.sql.Types.NVARCHAR:
					      {  
					    	  System.out.println("NULL value fr NVARCHAR column...");
					    	  Object nullObj = rs.getObject(column_name);
					    	  if (nullObj ==null) {
					    		  nullObj ="";
					    	  }
					    	  obj.put(column_name, nullObj);    break;
					      }				  
					      case java.sql.Types.VARCHAR:
					      {  System.out.println("NULL value VARCHAR column...");
						      Object nullObj = rs.getObject(column_name);
					    	  if (nullObj ==null) {
					    		  nullObj ="";
					    	  }
					    	  obj.put(column_name, nullObj);    break;
					     }
					      case java.sql.Types.TINYINT:
					        obj.put(column_name, rs.getInt(column_name));       break;
					      case java.sql.Types.SMALLINT:
					        obj.put(column_name, rs.getInt(column_name));       break;
					      case java.sql.Types.DATE:
					        obj.put(column_name, rs.getDate(column_name));      break;
					      case java.sql.Types.TIMESTAMP:
					        obj.put(column_name, rs.getTimestamp(column_name)); break;
					      case java.sql.Types.NULL:
					    	  System.out.println("NULL value column...");
					    	  Object nullObj = rs.getObject(column_name);
					    	  if (nullObj ==null) {
					    		  nullObj ="";
					    	  }
					    	  obj.put(column_name, nullObj);    break;
					      default:
					      {
					    	  obj.put(column_name, rs.getObject(column_name));    break;		    	
					      }
					    }//end switch 
				    	  
				      }////END for LOOP
				      json.put(obj);
				  }//End ResultSet loop
			    jsonString = json.toString();
			    String nStr = jsonString.replace("},", "},\n");
			    txtArea.setText(nStr);
			 } catch (SQLException e) {
						// TODO Auto-generated catch block
					e.printStackTrace();
			}	catch (JSONException e) {
					e.printStackTrace();
			}

		 }
	 
	 public void getDisplayXml(JTextArea txtArea, String selectedDB, Vector selectedTables, Vector selectedFields) throws ParserConfigurationException, TransformerException  {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();
		Element results = doc.createElement("Results");
		doc.appendChild(results);
			
		Connection conn= null;
		String user="sa";
		String pass ="administrator";	
		String xmlString = "";	
		String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
		try {
					conn = DriverManager.getConnection(dbURL);
				
			     Statement stmt = conn.createStatement();
			     
			     String [] arrSelectedFields =  (String[]) selectedFields.toArray(new String[selectedFields.size()] );
			     String joinedSelectedFields = String.join(",", arrSelectedFields);
			     
			   
			     String selectedTableValue = (String) selectedTables.get(0);
			     String query ="";
			     query = "Use "+selectedDB ;
			     query = query + " Select top (10) "+ joinedSelectedFields;
			     query = query + " From "+ selectedTableValue;		 
				
			     System.out.println(query);
			     ResultSet rs = stmt.executeQuery(query);
			
		 		ResultSetMetaData rsmd = rs.getMetaData(); 
			    //int numColumns = rsmd.getColumnCount();
		 		int numColumns = rsmd.getColumnCount();
			    System.out.println(numColumns);
		 		while (rs.next()) {
					Element row = doc.createElement("Row");
					results.appendChild(row);
					 
					for (int ii= 1; ii <= numColumns; ii++) {
						String columnName = rsmd.getColumnName(ii);
						Object value = rs.getObject(ii);
						//This is to validate if there is a null value in the results
						if (value==null) {
							value =  "null";
						}
						Element node = doc.createElement(columnName);
				    	node.appendChild(doc.createTextNode(value.toString()));
						row.appendChild(node); 
					}
				}
			    
			    DOMSource domSource = new DOMSource(doc);
			    TransformerFactory  tf = TransformerFactory.newInstance();
			    Transformer transformer = tf.newTransformer();
			    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			    transformer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");
			    StringWriter sw = new StringWriter();
			    StreamResult sr = new StreamResult(sw);
			    transformer.transform(domSource, sr);
			    System.out.println(sw.toString());
			    conn.close();
			    rs.close();
			    xmlString = sw.toString();
			    xmlString= xmlString.replaceAll("><", ">\n<");			    
			    txtArea.setText(xmlString);
		} catch (SQLException e) {
						// TODO Auto-generated catch block
					e.printStackTrace();
		}	catch (JSONException e) {
					e.printStackTrace();
		}	
	 } 
	
	 
	 public void getDisplayTable(JTable tableResult, String selectedDB, Vector selectedTables, Vector selectedFields) {	
			Connection conn= null;
			String user="sa";
			String pass ="administrator";	

			String dbURL = "jdbc:sqlserver://SUWANDA\\SQLEXPRESS:1433;databaseName=MyNewDB;user="+user+ ";password="+ pass+";encrypt=true;trustServerCertificate=true";
			try {
					conn = DriverManager.getConnection(dbURL);
					
				     Statement stmt = conn.createStatement();
				     
				     String [] arrSelectedFields =  (String[]) selectedFields.toArray(new String[selectedFields.size()] );
				     String joinedSelectedFields = String.join(",", arrSelectedFields);
				     String selectedTableValue = (String) selectedTables.get(0);
				     String query ="";
				     query = "Use "+selectedDB ;
				     query = query + " Select top (10) "+ joinedSelectedFields;
				     query = query + " From "+ selectedTableValue;		 
					 System.out.println(query);
				     ResultSet rs = stmt.executeQuery(query);
				     ResultSetMetaData rsmd = rs.getMetaData();
				     int numColumns = rsmd.getColumnCount();
				     System.out.println("NUMBER OF COLLL : "+ numColumns);
				     //GET COLUMN NAMES
				     Vector  columnNames = new Vector ();
				     for (int column = 1; column <= numColumns; column++) {
				    	 String colName = rsmd.getColumnName(column);
				    	 System.out.println("COL NAME ---: "+ colName);
				         columnNames.add(colName);
				     }
				   				
				    System.out.println(numColumns);
				    Vector data = new Vector();
			 		while (rs.next()) {
			 			Vector vec = new Vector();	
						for (int iii= 1; iii <= numColumns; iii++) {
							vec.add(rs.getObject(iii));
						}
						data.add(vec);
					}	    
			 	DefaultTableModel model = new DefaultTableModel(data, columnNames);	
			 	tableResult.setModel(model);
			} catch (SQLException e) {
							// TODO Auto-generated catch block
						e.printStackTrace();
			}	
		 } 
	 
  }	
