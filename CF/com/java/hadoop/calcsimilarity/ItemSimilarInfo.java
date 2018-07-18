package com.java.hadoop.calcsimilarity;

public class ItemSimilarInfo implements Comparable<Object> {

	public String item;
	public double occ;
	public double uv1;
	public double uv2;
	public double weight;
	
	public void getFromString(String line)
	{
		String[] user_infos = line.split("\t");
		if (user_infos.length >= 4)
		{
			this.item = user_infos[0];
			this.occ = Double.parseDouble(user_infos[1]);
			this.uv1 = Double.parseDouble(user_infos[2]);
			this.uv2 = Double.parseDouble(user_infos[3]);
			this.weight = occ / Math.sqrt(uv1 * uv2);
		}
		else if (user_infos.length == 2)
		{
			this.item = user_infos[0];
			this.occ = 0;
			this.uv1 = 0;
			this.uv2 = 0;
			this.weight = Double.parseDouble(user_infos[1]);
		}
		else
		{
			this.item = "error";
			this.occ = 0;
			this.uv1 = 0;
			this.uv2 = 0;
			this.weight = 0;
		}
	}
	
	public void getByFields(String item, String occ, String uv1, String uv2)
	{
		this.item = item;
		this.occ = Double.parseDouble(occ);
		this.uv1 = Double.parseDouble(uv1);
		this.uv2 = Double.parseDouble(uv2);
		this.weight = this.occ / Math.sqrt(this.uv1 * this.uv2);
	}
	
	public String toString()
	{
		return this.item + "\t" + Double.toString(this.weight);
	}
	
	public String toStringDebug()
	{
		String out = this.item + "\t" + Double.toString(this.weight)
				+ "\t" + Double.toString(this.occ)
				+ "\t" + Double.toString(this.uv1)
				+ "\t" + Double.toString(this.uv2);
		return out;
	}
	
	@Override
	public int compareTo(Object arg0) {
		// descend sort
		ItemSimilarInfo info = (ItemSimilarInfo)arg0;
		if (this.weight < info.weight)
			return 1;
		else if (this.weight > info.weight)
			return -1;
		else 
			return 0;
	}

}
