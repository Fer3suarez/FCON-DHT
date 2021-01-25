package es.upm.dit.dscc.DHT;

import java.io.Serializable;
import java.util.Arrays;

public class Operacion implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private int[] nodos;
	private int[] respuestas;
	private OperationsDHT operacion;
	/*
	 * 
	 * Al objeto Operacion hay que pasarle la operacion, los nodos y el numero de Réplicas
	 * 
	 */
	public Operacion(OperationsDHT operacion, int[] nodos, int nReplicas) {
		super();
		this.nodos = nodos;
		this.respuestas = new int[nReplicas]; 
		this.operacion = operacion; 
	}

	public int[] getNodos() {
		return nodos;
	}

	public void setNodos(int[] nodos) {
		this.nodos = nodos;
	}

	public int[] getRespuestas() {
		return respuestas;
	}

	public void setRespuestas(int[] respuestas) {
		this.respuestas = respuestas;
	}

	public OperationsDHT getOperacion() {
		return operacion;
	}

	public void setOperacion(OperationsDHT operacion) {
		this.operacion = operacion;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(nodos);
		result = prime * result + ((operacion == null) ? 0 : operacion.hashCode());
		result = prime * result + Arrays.hashCode(respuestas);
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
		Operacion other = (Operacion) obj;
		if (!Arrays.equals(nodos, other.nodos))
			return false;
		if (operacion == null) {
			if (other.operacion != null)
				return false;
		} else if (!operacion.equals(other.operacion))
			return false;
		if (!Arrays.equals(respuestas, other.respuestas))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Operacion [nodos=" + Arrays.toString(nodos) + ", respuestas=" + Arrays.toString(respuestas)
				+ ", operacion=" + operacion + "]";
	}
}
