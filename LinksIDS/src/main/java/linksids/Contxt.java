package linksids;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;


public class Contxt {

	static List<ContextElement> topList = null;
	static List<ContextElement> ceList = null;
	static ContextElement top = null;
	
	static ArrayList<CONTEXT>             iCL = new ArrayList<CONTEXT>();
	static ArrayList<CONTEXT_CONTEXT>    iCCL = new ArrayList<CONTEXT_CONTEXT>();


	public static void main(String args[]) {

		
		//initializeContext();
		System.out.println("toplist = " + topList);
		
		//add("Nederland" , "Noord-Holland", "Amsterdam", "Oost", "Zeeburg", "Cruquiusweg 31 hs", "Pakhuis Willem I");
		
		//System.exit(0);
		
		for(ContextElement ce1: topList){		
			printContextElement(ce1);
			for(ContextElement ce2: ce1.getChildren()){			
				printContextElement(ce2);
				for(ContextElement ce3: ce2.getChildren()){			
					printContextElement(ce3);
					for(ContextElement ce4: ce3.getChildren()){			
						printContextElement(ce4);
						for(ContextElement ce5: ce4.getChildren()){			
							printContextElement(ce5);
							for(ContextElement ce6: ce5.getChildren()){			
								printContextElement(ce6);
								for(ContextElement ce7: ce6.getChildren()){			
									printContextElement(ce7);
								}
							}
						}
					}
				}
			}
		}
	}

	public static void printTopList(){
		
		for(ContextElement ce1: topList){		
			printContextElement(ce1);
			for(ContextElement ce2: ce1.getChildren()){			
				printContextElement(ce2);
				for(ContextElement ce3: ce2.getChildren()){			
					printContextElement(ce3);
					for(ContextElement ce4: ce3.getChildren()){			
						printContextElement(ce4);
						for(ContextElement ce5: ce4.getChildren()){			
							printContextElement(ce5);
							for(ContextElement ce6: ce5.getChildren()){			
								printContextElement(ce6);
								for(ContextElement ce7: ce6.getChildren()){			
									printContextElement(ce7);
								}
							}
						}
					}
				}
			}
		}
		
		
	}
	
	public static void initializeContext(Connection connection){
		

		List<CONTEXT> cList = new ArrayList<CONTEXT>();
		cList = loadContext(connection);
		ceList = createContextElementList(cList);
		List<CONTEXT_CONTEXT> ccList = new ArrayList<CONTEXT_CONTEXT>();
		ccList = loadContext_Context(connection);
		createContextHierarchy(ceList, ccList); // link ContextElements
		topList = createTopList(ceList);
		
		//printTopList();
		
	}

	public static List<ContextElement> createTopList(List<ContextElement> ceList){
		
		//ContextElement ce  = new ContextElement();
		//ce.types.add("Name");
		//ce.values.add("Nederland");
		//ce.types.add("Level");
		//ce.values.add("Country");
		//ce.setId_C(1);
		
		//ceList.add(0, ce); // add in front
		
		
		//for(ContextElement ce1: ceList){
		//	for(int i = 0; i < ce1.types.size(); i++){
		//		if(ce1.types.get(i).equals("Level")){
		//			if(ce1.values.get(i).equals("Province")){
		//				//System.out.println("Adding province!!!");
		//				ce.getChildren().add(ce1);
		//				ce1.setParent(ce);
		//				break;
		//			}
		//		}
		//	}
		//}
		
		ArrayList<ContextElement> topList = new ArrayList<ContextElement>();
		
		// Allocate top element
		
		ContextElement ce  = new ContextElement();
		ce.types.add("NAME");
		ce.values.add("Top CE");
		ce.types.add("LEVEL");
		ce.values.add("Top");
		
		topList.add(ce);
		
		for(ContextElement ce1: ceList){
			for(int i = 0; i < ce1.types.size(); i++){
				if(ce1.types.get(i).equals("LEVEL")){
					if(ce1.values.get(i).equals("Country")){
						ce.getChildren().add(ce1);
						
					}
				}
			}
		}
						
		top = ce;
		return topList;
		
	}
	
	
	public static void printContextElement(ContextElement ce){
		
		String level    = null;
		String name     = null;
		String street   = null;
		String num      = null;
		String add      = null;
		String seqno    = null;
		String period   = null;

		for(int i = 0; i < ce.types.size(); i++){
			if(ce.types.get(i).equals("LEVEL"))
				level = ce.values.get(i);
			if(ce.types.get(i).equals("NAME"))
				name = ce.values.get(i);
			if(ce.types.get(i).equals("STREET"))
				street = ce.values.get(i);
			if(ce.types.get(i).equals("QUARTER"))
				street = ce.values.get(i);
			if(ce.types.get(i).equals("HOUSE_NUMBER"))
				num = ce.values.get(i);
			if(ce.types.get(i).equals("PERIOD"))
				period = ce.values.get(i);
			if(ce.types.get(i).equals("SEQUENCE_NUMBER"))
				seqno = ce.values.get(i);
			
		}
		
		if(street != null){
			name = street;
			if(num != null){
				name = name + " " + num;
				if(add != null){
					name = name + " " + add;
				}
			}
		}
		
		if(level.equals("Source")){
			if(seqno != null)
				name = name + " " + seqno;
			if(period != null)
				name = name + " " + period;
			
		}
		
		//if(name == null)
			//System.out.println("ce with Id_C = " + ce.getId_C() + " Level = " + level + "has no NAME!");
		
		//if(1==1) return;
		
		
		String indent = "     ";
		String prefix = null;
		if(level.equals("Top"))          prefix  = "";
		if(level.equals("Country"))      prefix  = indent;
		if(level.equals("Province"))     prefix  = indent + indent;
		if(level.equals("Municipality")) prefix  = indent + indent + indent;
		if(level.equals("Locality"))     prefix  = indent + indent + indent + indent;
		if(level.equals("Quarter"))      prefix  = indent + indent + indent + indent + indent;
		if(level.equals("Address"))      prefix  = indent + indent + indent + indent + indent;
		if(level.equals("Source"))       prefix  = indent + indent + indent + indent + indent;
		if(level.equals("Name"))         prefix  = indent + indent + indent + indent + indent + indent + indent;
		
		System.out.println(prefix + name);
		
		if(1==1)
			return;
		
		
		//if(ce.getId_C() <= Utils.getOld_id_C()) return;
		
		System.out.println();
		
		
		do{		
			System.out.println("Id_C = " + ce.getId_C());

			for(int i = 0; i < ce.types.size(); i++){

				System.out.println(ce.types.get(i) + " = " + ce.values.get(i));

			}
			ce = ce.getParent();
		} while(ce != null); 
	}
	
	
	public static ArrayList<ContextElement> createContextElementList(List<CONTEXT> cList){
		
		ArrayList<ContextElement> ceList = new ArrayList<ContextElement>();
		
		Collections.sort(cList, new Comparator<CONTEXT>()
				{
			public int compare(CONTEXT c1, CONTEXT c2){
				if(c1.getId_C() < c2.getId_C())
					return -1;
				if(c1.getId_C() > c2.getId_C())
					return 1;
				return 0;
			}
				});
		
		
		int curr_Id_C = -1;
		
		ContextElement ce = null;
		for(int i = 0; i < cList.size(); i++){			
			
			if(cList.get(i).getId_C() != curr_Id_C){
				curr_Id_C = cList.get(i).getId_C();
				//System.out.println("Id_C = " + curr_Id_C);
				if(ce != null)
					ceList.add(ce);
				ce = new ContextElement();
				ce.setId_C(cList.get(i).getId_C());
			}
			
			ce.types.add(cList.get(i).getType());
			ce.values.add(cList.get(i).getValue());
			
		}
		if(ce != null)
			ceList.add(ce);
		
		Utils.setOld_id_C(curr_Id_C);
		System.out.println("Highest Old Id_C = " + curr_Id_C);
		Utils.setId_C(curr_Id_C + 1); // for new elements
		
		return ceList;
		
	}
	
	public static void createContextHierarchy(List<ContextElement> ceList, List<CONTEXT_CONTEXT> ccList){
		
		
		for(CONTEXT_CONTEXT cc: ccList){
			
			ContextElement ceLower = new ContextElement();
			ceLower.setId_C(cc.getId_C_1());
			//System.out.println("Id_C1 = " + cc.getId_C_1());
			
			int i = Collections.binarySearch(ceList, ceLower, new Comparator<ContextElement>()
					{
				public int compare(ContextElement ce1, ContextElement ce2){
					if(ce1.getId_C() < ce2.getId_C())
						return -1;
					if(ce1.getId_C() > ce2.getId_C())
						return 1;
					return 0;
				}
					});
			
			//System.out.println(i);
			
			ContextElement ceHigher = new ContextElement();
			ceHigher.setId_C(cc.getId_C_2());
			//System.out.println("Id_C2 = " + cc.getId_C_2());
			
			int j = Collections.binarySearch(ceList, ceHigher, new Comparator<ContextElement>()
					{
				public int compare(ContextElement ce1, ContextElement ce2){
					if(ce1.getId_C() < ce2.getId_C())
						return -1;
					if(ce1.getId_C() > ce2.getId_C())
						return 1;
					return 0;
				}
					});

			//System.out.println(j);
			
			ceList.get(j).children.add(ceList.get(i));
			ceList.get(i).parent = ceList.get(j);
		}		
	}
	

	public static List<CONTEXT>  loadContext(Connection connection){
		
		Statement s = null;
		ResultSet resultSet = null;
		ArrayList<CONTEXT> a = new ArrayList<CONTEXT>();

		try {
			s = (Statement) connection.createStatement ();
			s.executeQuery("select * from links_ids.context");
			resultSet = s.getResultSet();
			
			while (resultSet.next ()){
				
				CONTEXT context = new CONTEXT();
				
				context.setId_D      (resultSet.getString("Id_D"));
				context.setId_C      (resultSet.getInt("Id_C"));
				context.setSource    (resultSet.getString("source"));
				context.setType      (resultSet.getString("type"));
				context.setValue     (resultSet.getString("value"));
				
				context.setDate_type (resultSet.getString("date_type"));
				context.setEstimation(resultSet.getString("estimation"));

				context.setDay       (resultSet.getInt("day"));
				context.setMonth     (resultSet.getInt("month"));
				context.setYear      (resultSet.getInt("year"));
				
				a.add(context);

				
			}
			
			resultSet.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Utils.executeQ(connection, "UNLOCK TABLES");
		
		return a;
		
		
	}

	public static List<CONTEXT_CONTEXT> loadContext_Context(Connection connection){
		
		Statement s = null;
		ResultSet resultSet = null;
		ArrayList<CONTEXT_CONTEXT> a = new ArrayList<CONTEXT_CONTEXT>();

		try {
			s = (Statement) connection.createStatement ();
			s.executeQuery("select * from links_ids.context_context");
			resultSet = s.getResultSet();
			
			while (resultSet.next ()){
				
				//System.out.println("Loading context_context");
				
				CONTEXT_CONTEXT cc = new CONTEXT_CONTEXT();
				
				cc.setId_D      (resultSet.getString("Id_D"));
				cc.setId_C_1    (resultSet.getInt("Id_C_1"));
				cc.setId_C_2    (resultSet.getInt("Id_C_2"));
				cc.setSource    (resultSet.getString("source"));
				cc.setRelation  (resultSet.getString("relation"));
				
				cc.setDate_type (resultSet.getString("date_type"));
				cc.setEstimation(resultSet.getString("estimation"));

				cc.setDay       (resultSet.getInt("day"));
				cc.setMonth     (resultSet.getInt("month"));
				cc.setYear      (resultSet.getInt("year"));
				
				a.add(cc);

				
			}
			
			resultSet.close();	
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Utils.executeQ(connection, "UNLOCK TABLES");
		
		return a;
		
		
	}

	// Routines for writing CONTEXT and CONTEXT_CONTEXT
	
	public static void addContext(Connection connection, CONTEXT context){
		
		iCL.add(context);
		if(iCL.size() >= 10000){
			writeCList(connection);
			iCL.clear();
		}
		
	}

	public static void addContextContext(Connection connection, CONTEXT_CONTEXT cc){
		
		iCCL.add(cc);
		if(iCCL.size() >= 10000){
			writeCCList(connection);
			iCCL.clear();
		}
		
	}


	private static void writeCList(Connection connection){
		
		String insertStatement =
				"insert into links_ids.context (Id_D, Id_C, Source, Type, Value, date_type, estimation, day, month, year) values(\"";
		
		for(CONTEXT c: iCL){
			
			// Front part
			
			insertStatement += c.getId_D();
			insertStatement += "\", \"";
			
			insertStatement += c.getId_C();
			insertStatement += "\", \"";
			
			insertStatement += c.getSource();
			insertStatement += "\", \"";
			
			insertStatement += c.getType();
			insertStatement += "\", \"";
			
			insertStatement += c.getValue();
			insertStatement += "\", \"";			
						
			
			// Timestamp part

			insertStatement += c.getDate_type();
			insertStatement += "\", \"";
			
			insertStatement += c.getEstimation();
			insertStatement += "\", \"";
			
			insertStatement += c.getDay();
			insertStatement += "\", \"";
			
			insertStatement += c.getMonth();
			insertStatement += "\", \"";
			
			insertStatement += c.getYear();
			insertStatement += "\"), (\"";
			
		}
		
		insertStatement = insertStatement.substring(0, insertStatement.length() - 4);
		
		//Utils.executeQ(connection, insertStatement);
		
		try {
			Statement s = (Statement) connection.createStatement ();
			System.out.println(insertStatement.substring(0,200));
			System.out.println(" Inserted number of linex: " + s.executeUpdate(insertStatement));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Utils.closeConnection(connection);
			e.printStackTrace();
		}
		
	}
	
	private static void writeCCList(Connection connection){
		
		String insertStatement =
				"insert into links_ids.context_context (Id_D, Id_C_1, Id_C_2, Source, relation, date_type, estimation, day, month, year) values(\"";
		
		for(CONTEXT_CONTEXT cc: iCCL){
			
			// Front part
			
			insertStatement += cc.getId_D();
			insertStatement += "\", \"";
			
			insertStatement += cc.getId_C_1();
			insertStatement += "\", \"";
			
			insertStatement += cc.getId_C_2();
			insertStatement += "\", \"";
			
			insertStatement += cc.getSource();
			insertStatement += "\", \"";
			
			insertStatement += cc.getRelation();
			insertStatement += "\", \"";
			
			
			// Timestamp part

			insertStatement += cc.getDate_type();
			insertStatement += "\", \"";
			
			insertStatement += cc.getEstimation();
			insertStatement += "\", \"";
			
			insertStatement += cc.getDay();
			insertStatement += "\", \"";
			
			insertStatement += cc.getMonth();
			insertStatement += "\", \"";
			
			insertStatement += cc.getYear();
			insertStatement += "\"), (\"";
			
		}
		
		insertStatement = insertStatement.substring(0, insertStatement.length() - 4);
		
		try {
			Statement s = (Statement) connection.createStatement ();
			System.out.println(insertStatement.substring(0,250));
			s.execute(insertStatement);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Utils.closeConnection(connection);
			e.printStackTrace();
		}
		
	}
	

	
	
	
	
	
	
	public static ContextElement get(int municipalityCode){
		
		//System.out.println("Municipality Code = " + municipalityCode);
		
		//System.out.println("CEList size = " + ceList.size());
		
		for(ContextElement ce: ceList){
			for(int i = 0; i < ce.getTypes().size(); i++){
				//System.out.println("Type = " + ce.getTypes().get(i));
				if(ce.getTypes().get(i).equals("HSN MUNICIPALITY CODE")){
					int x = new Integer(ce.getValues().get(i));
					//System.out.println("code = " + x);
					if(x == municipalityCode)
						return ce;
				}
			}
		}

		return null;
		
		
		//ContextElement ce = new ContextElement();
		//ce.setId_C(Id_C);
		
		//int i = Collections.binarySearch(ceList, ce, new Comparator<ContextElement>()
		//		{
		//	public int compare(ContextElement ce1, ContextElement ce2){
		//		if(ce1.getId_C() < ce2.getId_C())
		//			return -1;
		//		if(ce1.getId_C() > ce2.getId_C())
		//			return 1;
		//		return 0;
		//	}
		//		});
		
				
	}
	
	public static ContextElement get2(String municipality){
		
		for(ContextElement ce: ceList){
		
			String name = null;
			String level = null;
			for(int i = 0; i < ce.getTypes().size(); i++){
				//System.out.println("Type = " + ce.getTypes().get(i));
				if(ce.getTypes().get(i).equals("NAME"))
				   name = ce.getValues().get(i);
				if(ce.getTypes().get(i).equals("LEVEL"))
				   level = ce.getValues().get(i);
				if(name != null && level != null) break;
			}

			//System.out.println("    Name = " + name);
			if(level.equalsIgnoreCase("Municipality")){ 
					if(name.trim().equalsIgnoreCase(municipality.trim()))
						return ce;
			}
		}
		
		// We add the municipality to the context system
		ContextElement ceNew = new ContextElement();
		ceNew.getTypes().add("LEVEL");
		ceNew.getValues().add("Municipality");
		ceNew.getTypes().add("NAME");
		ceNew.getValues().add(municipality.trim());

		// Link municipality directly to country (Unknown)

		ContextElement ceParent = null;
      	for(ContextElement ce1: topList.get(0).getChildren()){
			//System.out.println("ce = " + ce1);
			String name = null;
			String level = null;
			for(int i = 0; i < ce1.getTypes().size(); i++){
				if(ce1.getTypes().get(i).equals("NAME"))
					   name = ce1.getValues().get(i);
				if(ce1.getTypes().get(i).equals("LEVEL"))
					   level = ce1.getValues().get(i);
			}
			if(level.equalsIgnoreCase("Country") && name.equalsIgnoreCase("Unknown")){
				
				ceParent = ce1;
				break;
				
			}
		}
		//System.out.println("ceParent = " + ceParent);
		if(ceParent != null){
			ceNew.setId_C(Utils.getId_C());
			//System.out.println("New element " + municipality.trim() + " has id_c = " + ceNew.getId_C());
			ceParent.getChildren().add(ceNew);
			ceNew.setParent(ceParent);
			ceList.add(ceNew);
			return ceNew;

		}
		//System.out.println("    Name = " + municipality);// we left the municipality section
		
		return null;
		
	}
	
	public static void save(Connection connection) throws SQLException{
		
		System.out.println("Highest Id_C = " + (Utils.getId_C() -1));
		System.out.println("Connection is closed is  " + connection.isClosed());
		
		
		//printTopList();
		
		//if(1==1)
		//return;
		
		for(ContextElement ce1: topList){		
			if(ce1.getId_C() > Utils.getOld_id_C()){
				ce1.save(connection);		
				link(ce1, top, connection);
			}
			for(ContextElement ce2: ce1.getChildren()){			
				if(ce2.getId_C() > Utils.getOld_id_C()){
					ce2.save(connection);
					link(ce2, ce1, connection);

				}
				for(ContextElement ce3: ce2.getChildren()){			
					if(ce3.getId_C() > Utils.getOld_id_C()){
						ce3.save(connection);
						link(ce3, ce2, connection);
					}
					for(ContextElement ce4: ce3.getChildren()){			
						if(ce4.getId_C() > Utils.getOld_id_C()){
							ce4.save(connection);
							link(ce4, ce3, connection);
						}
						for(ContextElement ce5: ce4.getChildren()){			
							if(ce5.getId_C() > Utils.getOld_id_C()){
								ce5.save(connection);
								link(ce5, ce4, connection);

							}
							for(ContextElement ce6: ce5.getChildren()){			
								if(ce6.getId_C() > Utils.getOld_id_C()){
									ce6.save(connection);
									link(ce6, ce5, connection);

								}
								for(ContextElement ce7: ce6.getChildren()){			
									if(ce7.getId_C() > Utils.getOld_id_C()){
										ce7.save(connection);
										link(ce7, ce6, connection);
									}
								}
							}
						}
					}
				}
			}
		}

	}

	
	public static void link(ContextElement ce1, ContextElement ce2, Connection connection){
		
		CONTEXT_CONTEXT cc = new CONTEXT_CONTEXT();
		cc.setId_C_1(ce1.getId_C());
		cc.setId_C_2(ce2.getId_C());
		
		String level1 = null;
		
		for(int i = 0; i < ce1.getTypes().size(); i++){
			//System.out.println(ce1.getTypes().get(i) + " = " + ce1.getValues().get(i));
			if(ce1.getTypes().get(i).equals("LEVEL")){
				level1 = ce1.getValues().get(i);
				break;
			}
		}
		
		String level2 = null;
		
		for(int i = 0; i < ce2.getTypes().size(); i++){
			if(ce2.getTypes().get(i).equals("LEVEL")){
				level2 = ce2.getValues().get(i);
				break;
			}
		}
		
		//System.out.println("level1 = " + level1);
		
		cc.setRelation(level1 + " in " + level2);
		addContextContext(connection, cc);
		
		
	}

	public static ContextElement locateCertificate(String source, int yearCertificate, String sequenceNumberCertificate,  ContextElement ce1, String   level){

		for(ContextElement ce: ce1.getChildren()){
			
			String source1 = null;
			int yearCertificate1 = 0;
			String sequenceNumberCertificate1 = null;
			String level1 = null;
			
			for(int i = 0; i < ce.types.size(); i++){
				if(ce.types.get(i).compareTo("LEVEL") == 0)
					level1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("NAME") == 0)
					source1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("PERIOD") == 0)
					yearCertificate1 = new Integer(ce.values.get(i));
				if(ce.types.get(i).compareTo("SEQUENCE_NUMBER") == 0)
					sequenceNumberCertificate1 = ce.values.get(i);
				
			}

			if(comp(level1, "Source") == true && comp(source1, source) == true && yearCertificate1 == yearCertificate && 
					comp(sequenceNumberCertificate1, sequenceNumberCertificate) == true)
				return ce;
		}
			
		ContextElement ce = new ContextElement();
		ce.types.add("LEVEL");
		ce.values.add(level);
		ce.types.add("NAME");
		ce.values.add(source);
		ce.types.add("PERIOD");
		ce.values.add("" + yearCertificate);
		ce.types.add("SEQUENCE_NUMBER");
		ce.values.add("" + sequenceNumberCertificate);
		
		ce.setId_C(Utils.getId_C());
		ce.setParent(ce1);
		
		ce1.getChildren().add(ce);

		return ce;

		
	}
	
	public static ContextElement locateAddress(String street, String number, String addition, ContextElement ce1, String   level){
		
		
		if(street == null)
			return null;
		
		//System.out.println("street: " + street + " number: " + number + " addition: " + addition + "level = " + level);
		
		// Try to find address
		
		for(ContextElement ce: ce1.getChildren()){
			
			String street1 = null;
			String number1 = null;
			String addition1 = null;
			String level1 = null;
			
			for(int i = 0; i < ce.types.size(); i++){
				if(ce.types.get(i).compareTo("LEVEL") == 0)
					level1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("STREET") == 0)
					street1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("HOUSE_NUMBER") == 0)
					number1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("HOUSE_NUMBER_ADDITION") == 0)
					addition1 = ce.values.get(i);
				
			}

			if(comp(level1, "Address") == true && comp(street1, street) == true && comp(number1, number) == true && comp(addition1, addition) == true)
				return ce;

			
		}
		
		//System.out.println("Not found");
		
		ContextElement ce = new ContextElement();
		ce.types.add("LEVEL");
		ce.values.add(level);
		ce.types.add("STREET");
		ce.values.add(street);
		if(number != null){
			ce.types.add("HOUSE_NUMBER");
			ce.values.add(number);
		}
		if(addition != null){
			ce.types.add("HOUSE_NUMBER_ADDITION");
			ce.values.add(addition);
		}
		
		ce.setId_C(Utils.getId_C());
		ce.setParent(ce1);
		
		ce1.getChildren().add(ce);

		return ce;

			
	}
	
	public static ContextElement locateQuarter(String quarter, String number, String addition, ContextElement ce1, String   level){
		
		
		if(quarter == null)
			return null;
		
		//System.out.println("street: " + street + " number: " + number + " addition: " + addition + "level = " + level);
		
		// Try to find address
		
		for(ContextElement ce: ce1.getChildren()){
			
			String quarter1 = null;
			String number1 = null;
			String addition1 = null;
			String level1 = null;
			
			for(int i = 0; i < ce.types.size(); i++){
				if(ce.types.get(i).compareTo("LEVEL") == 0)
					level1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("QUARTER") == 0)
					quarter1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("HOUSE_NUMBER") == 0)
					number1 = ce.values.get(i);
				if(ce.types.get(i).compareTo("HOUSE_NUMBER_ADDITION") == 0)
					addition1 = ce.values.get(i);
				
			}

			if(comp(level1, "Quarter") == true && comp(quarter1, quarter) == true && comp(number1, number) == true && comp(addition1, addition) == true)
				return ce;

			
		}
		
		//System.out.println("Not found");
		
		ContextElement ce = new ContextElement();
		ce.types.add("LEVEL");
		ce.values.add(level);
		ce.types.add("QUARTER");
		ce.values.add(quarter);
		if(number != null){
			ce.types.add("HOUSE_NUMBER");
			ce.values.add(number);
		}
		if(addition != null){
			ce.types.add("HOUSE_NUMBER_ADDITION");
			ce.values.add(addition);
		}
		
		ce.setId_C(Utils.getId_C());
		ce.setParent(ce1);
		
		ce1.getChildren().add(ce);

		return ce;

			
	}
	
	
	public static ContextElement locate(String s, ContextElement ce1, String   level){
				
		// System.out.println("Locate " + s + " in " + level);

		for(ContextElement ce: ce1.getChildren()){
			
			
			String name = null;
			String lvl = null;
			
			for(int i = 0; i < ce.types.size(); i++){
				if(ce.types.get(i).compareTo("NAME") == 0)
					name = ce.values.get(i);
				if(ce.types.get(i).compareTo("LEVEL") == 0)
					lvl = ce.values.get(i);
			}


			if(name != null && lvl != null && name.equalsIgnoreCase(s) && level.equals(lvl)){
				//System.out.println("Found, level = " + level + " name = " + s);

				return ce;
			}


		}
		
		//System.out.println("Not Found, level = " + level + " name = " + s);

		ContextElement ce = new ContextElement();
		ce.types.add("NAME");
		ce.values.add(s);
		ce.types.add("LEVEL");
		ce.values.add(level);
		ce.setId_C(Utils.getId_C());
		ce.setParent(ce1);
		
		ce1.getChildren().add(ce);

		return ce;



	}

	private static char randChar(){

		Double a = Math.random();
		String s = a.toString();
		char c = s.charAt(3);

		return c;



	}

	private static boolean comp (String s1, String s2){
		
		//System.out.println("s1 = " + s1 + " s2 = " + s2);
		
		if(s1 == null && s2 == null)
			return true;
		if(s1 != null && s2 != null && s1.trim().equalsIgnoreCase(s2.trim()) == true)
			return true;
		return false;
	}

	private static String normalize(String s){
		
		if(s == null) return s; // null is normalized
		else{
			s = s.trim();
			if(s.length() == 0)
				return null;
			else 
				return s;
		}
	}

	
	public static int addCertificate(String Country, String Province, String Municipality, String source, int yearCertificate, String sequenceNumberCertificate){
		
		if(Country != null){
			ContextElement contextCountry = locate(Country, top, "Country");
			if(Province != null){
				ContextElement contextProvince = locate(Province, contextCountry, "Province");
				if(Municipality != null){
					ContextElement contextMunicipality = locate(Municipality, contextProvince, "Municipality");
					ContextElement contextCertificate = locateCertificate(source, yearCertificate, sequenceNumberCertificate, contextMunicipality, "Source");
					return contextCertificate.getId_C();
				}
			}
		}
		
		return 0;
	}
	
	public static int add(String Country, String Province, String Municipality, String Locality, String Quarter, String Street, String Number, String Addition, String  Name){
		
		Country      = normalize(Country);     
		Province     = normalize(Province);     
		Municipality = normalize(Municipality);     
		Locality     = normalize(Locality);     
		Quarter      = normalize(Quarter);     
		Street       = normalize(Street);     
		Number       = normalize(Number);     
		Addition     = normalize(Addition);     
		Name         = normalize(Name);    


		
		if(Country != null){
			ContextElement contextCountry = locate(Country, top, "Country");
			if(Province != null){
				ContextElement contextProvince = locate(Province, contextCountry, "Province");
				if(Municipality != null){
					ContextElement contextMunicipality = locate(Municipality, contextProvince, "Municipality");
					if(Locality != null){
						ContextElement contextLocality = locate(Locality, contextMunicipality, "Locality");
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextLocality, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition,  contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextLocality, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextLocality, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextLocality.getId_C();
								}
							}
						}
					}  
					else{ // No locality
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextMunicipality, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextMunicipality, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextMunicipality, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextMunicipality.getId_C();
								}
							}
						}
					}
				}		
				else{ // No Municipality
					if(Locality != null){
						ContextElement contextLocality = locate(Locality, contextProvince, "Locality");
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextLocality, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextLocality, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextLocality, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextLocality.getId_C();
								}
							}
						}
					}  
					else{ // No locality
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextProvince, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextProvince, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextProvince, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextProvince.getId_C();
								}
							}
						}
					}
				}
			}
			else{ // No Province
				if(Municipality != null){
					ContextElement contextMunicipality = locate(Municipality, contextCountry, "Municipality");
					if(Locality != null){
						ContextElement contextLocality = locate(Locality, contextMunicipality, "Locality");
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextLocality, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextLocality, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextLocality, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextLocality.getId_C();
								}
							}
						}
					}  
					else{ // No locality
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextMunicipality, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextMunicipality, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextMunicipality, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextMunicipality.getId_C();
								}
							}
						}
					}
				}		
				else{ // No Municipality
					if(Locality != null){
						ContextElement contextLocality = locate(Locality, contextCountry, "Locality");
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextLocality, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextLocality, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextLocality, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextLocality.getId_C();
								}
							}
						}
					}  
					else{ // No locality
						if(Quarter != null){
							ContextElement contextQuarter = locateQuarter(Quarter, Number, Addition, contextCountry, "Quarter");
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextQuarter, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextQuarter, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextQuarter.getId_C();
								}
							}
						}
						else{ //No quarter
							if(Street != null){
								ContextElement contextAddress = locateAddress(Street, Number, Addition, contextCountry, "Address");
								if(Name != null){
									ContextElement contextName = locate(Name, contextAddress, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextAddress.getId_C();
								}
							}
							else{ // No Street
								if(Name != null){
									ContextElement contextName = locate(Name, contextCountry, "Name");
									return contextName.getId_C();
								}
								else{ // No Name
									return contextCountry.getId_C();
								}
							}
						}
					}
				}
			}
		}
		else{ // No Country
			
		}
		
		
		return 0;
		
	}








}
