package linksids;

import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.Connection;

public class ContextElement {
	
	String name;
	int level;
	
	int Id_C;
	
	ArrayList<String> types  = new ArrayList<String>();
	ArrayList<String> values = new ArrayList<String>();
	
	ContextElement parent;
	
	List<ContextElement> children = new ArrayList<ContextElement>();
	List<ContextElement> internalChildren = new ArrayList<ContextElement>();
	
	
	public void save(Connection connection){
		
		
		for(int i = 0; i < getTypes().size(); i++){
			
		 
			//System.out.println("Persisting Context");
			
			CONTEXT context = new CONTEXT();
			
			context.setId_C(getId_C());
			context.setId_D("LINKS");
			
			//System.out.println(getTypes().get(i));
			context.setType(getTypes().get(i));
			context.setValue(getValues().get(i));
			//System.out.println(getValues().get(i));

			//em.persist(context);
			Utils.addContext(connection, context);
			
		}	
		

		//for(ContextElement ce: getInternalChildren()){
			
			//ce.save(em);
			//Contxt.link(ce, this, em);
			
			
		//}
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	

	public ContextElement getParent() {
		return parent;
	}

	public void setParent(ContextElement parent) {
		this.parent = parent;
	}

	public List<ContextElement> getChildren() {
		return children;
	}

	public void setChildren(List<ContextElement> children) {
		this.children = children;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public int getId_C() {
		return Id_C;
	}

	public void setId_C(int id_C) {
		Id_C = id_C;
	}

	public ArrayList<String> getTypes() {
		return types;
	}

	public void setTypes(ArrayList<String> types) {
		this.types = types;
	}

	public ArrayList<String> getValues() {
		return values;
	}

	public void setValues(ArrayList<String> values) {
		this.values = values;
	}

	public List<ContextElement> getInternalChildren() {
		return internalChildren;
	}

	public void setInternalChildren(List<ContextElement> internalChildren) {
		this.internalChildren = internalChildren;
	} 
	
}
