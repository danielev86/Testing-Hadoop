package esercizio2;


import java.text.ParseException;



public class Scontrino {
	
	private String ingrediente;
	private String data;
	private int valore;

	

	public Scontrino(String ingrediente, String data, int valore) {
		super();
		this.ingrediente = ingrediente;
		this.data = data;
		this.valore = valore;
	}



	public String getIngrediente() {
		return ingrediente;
	}



	public void setIngrediente(String ingrediente) {
		this.ingrediente = ingrediente;
	}



	public String getData() {
		return data;
	}



	public void setData(String data) {
		this.data = data;
	}



	public int getValore() {
		return valore;
	}



	public void setValore(int valore) {
		this.valore = valore;
	}



	public Scontrino() {
		valore=0;
		
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((ingrediente == null) ? 0 : ingrediente.hashCode());
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Scontrino other = (Scontrino) obj;
		if (ingrediente == null) {
			if (other.ingrediente != null)
				return false;
		} else if (!ingrediente.equals(other.ingrediente))
			return false;
		return true;
	}
	
	
	

}
