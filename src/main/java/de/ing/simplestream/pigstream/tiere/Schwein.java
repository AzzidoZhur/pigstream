package de.ing.simplestream.pigstream.tiere;


public class Schwein {
	
	private String name;
	private int gewicht;
	
	public Schwein() {
		this("nobody",10);
	}
	
	public Schwein(String name, int gewicht) {
		super();
		this.name = name;
		this.gewicht = gewicht;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getGewicht() {
		return gewicht;
	}

	public void setGewicht(int gewicht) {
		this.gewicht = gewicht;
	}
	
	public void fressen() {
		gewicht ++;
	}

	@Override
	public String toString() {
		return "Schwein [name=" + name + ", gewicht=" + gewicht + "]";
	}
	
	

}
