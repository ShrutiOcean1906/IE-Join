package tests;

import btree.BTreeFile;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.util.Vector;

class IEJoinDriver_2c extends TestDriver implements GlobalConst {
	private static final int NUM_PAGES = 30;

	//private ArrayList<R> qTuples = new ArrayList<>();
	private Heapfile inputHeapFile;
	private AttrType[] inputAttrTypes;
	private static final String doublePredOutput = "double_pred_output.txt";
	JoinsDriver jd ;
	public void IEJoinDriver1(){
		try {
            //Load the table R from file to the memory

            FileReader fr = new FileReader(jd.relName[0]+".txt");
            BufferedReader br = new BufferedReader(fr);

            br.readLine(); //Read the file header


            inputHeapFile = new Heapfile("reserves2.in");

            //Initialize a tuple object for inserting into the heapfile
            Tuple t = new Tuple();

            //Define the attributes for the Tuple(R)
            inputAttrTypes= new AttrType[4];
            for(int i=0;i<inputAttrTypes.length;i++){
                inputAttrTypes[i] = new AttrType(AttrType.attrInteger);
            }

            t.setHdr((short) 4, inputAttrTypes, null);
            while(true){
                String line = br.readLine();
                if(line!=null){
                    String[] fields = line.split(",");
                    if(fields.length < 4){
                        continue;
                    }

                    // Set the fields of the tuple
                    for(int i=0; i<fields.length; i++){
                        t.setIntFld(i+1,Integer.parseInt(fields[i]));
                    }

                    // Insert the tuple into the HeapFile
                    inputHeapFile.insertRecord(t.getTupleByteArray());

                }else{
                    break;
                }
            }

            br.close();
            fr.close();

            //ieSelfJoin(new int[]{1},new int[]{3}, new int[]{3});
            System.out.println("heapR Size: " + inputHeapFile.getRecCnt());

        } catch (Exception e) {
            e.printStackTrace();
        }

        inputHeapFile = null;

        try {
            //Load the table S from file to the memory

            FileReader fr = new FileReader(jd.relName[1]+".txt");
            BufferedReader br = new BufferedReader(fr);

            br.readLine(); //Read the file header

            //initDB();

            inputHeapFile = new Heapfile("sailors2.in");

            //Initialize a tuple object for inserting into the heapfile
            Tuple t = new Tuple();

            //Define the attributes for the Tuple(R)
            inputAttrTypes= new AttrType[4];
            for(int i=0;i<inputAttrTypes.length;i++){
                inputAttrTypes[i] = new AttrType(AttrType.attrInteger);
            }

            t.setHdr((short) 4, inputAttrTypes, null);
            while(true){
                String line = br.readLine();
                if(line!=null){
                    String[] fields = line.split(",");
                    if(fields.length < 4){
                        continue;
                    }

                    // Set the fields of the tuple
                    for(int i=0; i<fields.length; i++){
                        t.setIntFld(i+1,Integer.parseInt(fields[i]));
                    }

                    // Insert the tuple into the HeapFile
                    inputHeapFile.insertRecord(t.getTupleByteArray());

                }else{
                    break;
                }
            }

            br.close();
            fr.close();

            //ieSelfJoin(new int[]{1},new int[]{3}, new int[]{3});
            System.out.println("heapS Size: " + inputHeapFile.getRecCnt());

        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	public IEJoinDriver_2c(JoinsDriver jd){
		this.jd = jd;
		String nameRoot = "ieselfjoin";

		//dbpath = "/tmp/"+nameRoot+System.getProperty("user.name")+".minibase-db";
		//logpath = "/tmp/"+nameRoot +System.getProperty("user.name")+".minibase-log";
		//dbpath = "C:/Users/Mihir/Desktop/MS/SPRING 2016/CSE510-DBMSI/"+System.getProperty("user.name")+".minibase_3-db";
		//logpath = "C:/Users/Mihir/Desktop/MS/SPRING 2016/CSE510-DBMSI/"+System.getProperty("user.name")+".minibase_3-log";
		dbpath = "/tmp/"+System.getProperty("user.name")+".minibase-db";
		logpath = "/tmp/"+System.getProperty("user.name")+".minibase-log";
		RID rid;
		Heapfile heapS = null;
		//Tuple t = new Tuple();
		SystemDefs sysdef = new SystemDefs( dbpath, 30000, NUMBUF, "Clock" );

		inputAttrTypes= new AttrType[4];
		for(int i=0;i<inputAttrTypes.length;i++){
			inputAttrTypes[i] = new AttrType(AttrType.attrInteger);
		}
		
		Heapfile heapR= null;
		IEJoinDriver1();
		System.out.println("After Copying....");
		try {
			heapS = new Heapfile("sailors2.in");
			heapR = new Heapfile("reserves2.in");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ieJoin(new int[]{jd.proj1,jd.proj2},new int[]{jd.col1_Prdt1,jd.col1_Prdt2}, new int[]{Integer.parseInt(jd.opxPrdctOne),Integer.parseInt(jd.opxPrdctTwo)}, heapR, heapS);
	}



	private void ieJoin(int[] outputColumns, int[] predicateColumns, int[] operator, Heapfile heapR, Heapfile heapS){
		try {
			// Ascending Descending flags for L1, L2, L1', L2'
			int m=0;
			try {
				m = heapR.getRecCnt();
			} catch (Exception e) {
				e.printStackTrace();
			}

			int n=0;
			try {
				n = heapS.getRecCnt();
			} catch (Exception e) {
				e.printStackTrace();
			}
			boolean ascFlagL1_ = false;
			boolean ascFlagL2_ = false;
			boolean ascFlagL1 = false;
			boolean ascFlagL2 = false;

			FldSpec [] Sprojection = new FldSpec[4];
			Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
			Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
			Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

			FldSpec [] Rprojection = new FldSpec[4];
			Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
			Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
			Rprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

			FileScan fscanR = new FileScan("reserves2.in", inputAttrTypes, null, (short)inputAttrTypes.length,
					Rprojection.length, Rprojection , null);

			FileScan fscanS = new FileScan("sailors2.in", inputAttrTypes, null, (short)inputAttrTypes.length,
					Sprojection.length, Sprojection, null);

			Sort sortR, sortS, sortR1, sortS1;

			Heapfile L1 = new Heapfile("L1"); //L1 Heap

			Heapfile L2 = new Heapfile("L2"); //L2 Heap

			Heapfile L1_ = new Heapfile("L1_"); // L1' Heap

			Heapfile L2_ = new Heapfile("L2_"); //L2' Heap

			/***
			 * L1 and L1' Implementation
			 */

			int[] l1 = new int[m];  // L1 Array
			int[] l1_ = new int[n];

			Map<Integer, Integer> indexMapR = new HashMap<Integer, Integer>();
			Map<Integer, Integer> indexMapS = new HashMap<Integer, Integer>();
			RID rid = null;
			List<RID> ridL1_ = new ArrayList<RID>();

			if(operator[0]==2||operator[0]==4){

				//L1
				sortR = new Sort(inputAttrTypes, (short)4, null, fscanR, predicateColumns[0] , new TupleOrder(TupleOrder.Descending),
						16 , (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL1 = false;
				int j = 0;
				Tuple rec12 = null;
				try {
					rec12 = sortR.get_next();
				} catch (Exception e2) {
						e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l1[j] = rec12.getIntFld(predicateColumns[0]);
						rid = L1.insertRecord(rec12.returnTupleByteArray());
						int key = rec12.getIntFld(1);
						indexMapR.put(key, j);
						rec12 = sortR.get_next();
						j++;

					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				sortR.close();

				//L1_
				sortS = new Sort(inputAttrTypes, (short)4, null, fscanS , predicateColumns[0] , new TupleOrder(TupleOrder.Descending),
						16, (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL1_ = false;
				j = 0;
				try {
					rec12 = sortS.get_next();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l1_[j] = rec12.getIntFld(predicateColumns[0]);
						rid = L1_.insertRecord(rec12.returnTupleByteArray());
						ridL1_.add(rid);
						// new implementation
						int key = rec12.getIntFld(1);
						indexMapS.put(key, j);
						j++;
						rec12 = sortS.get_next();
					}
					catch (Exception e1) {
						e1.printStackTrace();
					}
				}

				sortS.close();


			}else{
				//L1
				sortR = new Sort(inputAttrTypes, (short)4, null, fscanR, predicateColumns[0] , new TupleOrder(TupleOrder.Ascending),
						16, (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL1 = true;
				int j = 0;
				Tuple rec12 = null;
				try {
					rec12 = sortR.get_next();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l1[j] = rec12.getIntFld(predicateColumns[0]);
						rid = L1.insertRecord(rec12.returnTupleByteArray());
						// new implementation
						int key = rec12.getIntFld(1);
						indexMapR.put(key, j);
						rec12 = sortR.get_next();
						j++;

					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				sortR.close();


				//L1_

				sortS = new Sort(inputAttrTypes, (short)4, null, fscanS , predicateColumns[0] , new TupleOrder(TupleOrder.Ascending),
						16, (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL1_ = true;
				j = 0;
				try {
					rec12 = sortS.get_next();
				} catch (Exception e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l1_[j] = rec12.getIntFld(predicateColumns[0]);
						rid = L1_.insertRecord(rec12.returnTupleByteArray());
						ridL1_.add(rid);

						// new implementation
						int key = rec12.getIntFld(1);
						indexMapS.put(key, j);

						j++;
						//rec12.print(inputAttrTypes);
						rec12 = sortS.get_next();
					}
					catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				sortS.close();
				}
			/***
			 * L2 and L2' Implementation
			 */

			FileScan scanR = new FileScan("L1", inputAttrTypes, null, (short)inputAttrTypes.length,
					Rprojection.length, Rprojection , null);

			FileScan scanS = new FileScan("L1_", inputAttrTypes, null, (short)inputAttrTypes.length,
					Sprojection.length, Sprojection, null);

			int[] l2 = new int[m];  // L1 Array
			int[] P = new int[m];
			int[] P_ = new int[n];
			int[] l2_ = new int[n];

			if(operator[1]==2||operator[1]==4){

				// L2
				sortR1 = new Sort(inputAttrTypes, (short)4, null, scanR, predicateColumns[1] , new TupleOrder(TupleOrder.Ascending),
						16, (int)Math.floor(NUM_PAGES/2.0));
				ascFlagL2 = true;
				int j = 0;
				Tuple rec12 = new Tuple();
				try {
					rec12 = sortR1.get_next();
				} catch (Exception e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l2[j] = rec12.getIntFld(predicateColumns[1]);
						L2.insertRecord(rec12.returnTupleByteArray());

						// new implementation
						int key = rec12.getIntFld(1);
						int index = indexMapR.get(key);
						// end new implementation

						P[j++]= index;
						rec12 = sortR1.get_next();

					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				sortR1.close();


				// L2'
				sortS1 = new Sort(inputAttrTypes, (short)4, null, scanS , predicateColumns[1] , new TupleOrder(TupleOrder.Ascending),
						16, (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL2_ = true;
				j = 0;
				try {
					rec12 = sortS1.get_next();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l2_[j] = rec12.getIntFld(predicateColumns[1]);
						L2_.insertRecord(rec12.returnTupleByteArray());

						// new implementation
						int key = rec12.getIntFld(1);
						int index = indexMapS.get(key);
						// end new implementation

						P_[j]= index ;
						//rec12.print(inputAttrTypes);
						rec12 = sortS1.get_next();
						j++;
					}
					catch (Exception e1) {
						e1.printStackTrace();
					}
				}

				sortS1.close();


			}else{
				// L2
				sortR1 = new Sort(inputAttrTypes, (short)4, null, scanR, predicateColumns[1] , new TupleOrder(TupleOrder.Descending),
						4, (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL2 = false;

				int j = 0;
				Tuple rec12 = new Tuple();
				try {
					rec12 = sortR1.get_next();
				} catch (Exception e2) {

					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l2[j] = rec12.getIntFld(predicateColumns[1]);
						L2.insertRecord(rec12.returnTupleByteArray());
						// new implementation
						int key = rec12.getIntFld(1);
						int index = indexMapR.get(key);
						// end new implementation
						P[j++] = index;
						rec12 = sortR1.get_next();

					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				sortR1.close();

				// L2'
				sortS1 = new Sort(inputAttrTypes, (short)4, null, scanS , predicateColumns[1] , new TupleOrder(TupleOrder.Descending),
						4, (int)Math.floor(NUM_PAGES/2.0) );
				ascFlagL2_ = false;
				j = 0;
				try {
					rec12 = sortS1.get_next();
				} catch (Exception e2) {

					e2.printStackTrace();
				}
				while(rec12!= null) {
					try {
						l2_[j] = rec12.getIntFld(predicateColumns[1]);
						L2_.insertRecord(rec12.returnTupleByteArray());
						int key = rec12.getIntFld(1);
						int index = indexMapS.get(key);
						// end new implementation
						P_[j]= index ;
						rec12 = sortS1.get_next();
						j++;
					}
					catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				sortS1.close();
			}
			/***
			 * O1 and O2 Offset Calculation
			 */
			int[] o1 = new int[l1.length];
			int[] o2 = new int[l1.length];

			if (ascFlagL1_){
				for (int i=0;i<l1.length;i++){
					o1[i] = getIndexAsc(l1[i], l1_ , 0, l1_.length - 1);
				}
			}else{
				for (int i=0;i<l1.length;i++){
					o1[i] = getIndexDesc(l1[i], l1_ , 0, l1_.length - 1);
				}
			}

			if (ascFlagL2_){
				for (int i=0;i<l1.length;i++){
					o2[i] = getIndexAsc(l2[i], l2_, 0, l2_.length - 1) ;
				}
			}else{
				for (int i=0;i<l2.length;i++){
					o2[i] = getIndexDesc(l2[i], l2_, 0, l2_.length - 1) ;
				}
			}



			/********************************************************************************
			 * 							MAIN ALGORITHM BEGINS
			 ********************************************************************************/

			long startTime = System.nanoTime();

			FileScan scanL2 = new FileScan("L2", inputAttrTypes, null, (short)inputAttrTypes.length,
					Rprojection.length, Rprojection , null);
			Tuple tupR = new Tuple();
			tupR = scanL2.get_next();

			FileScan scanL1_ = new FileScan("L1_", inputAttrTypes, null, (short)inputAttrTypes.length,
					Sprojection.length, Sprojection, null);
			Tuple tupS = new Tuple();

			try {
				tupS.setHdr((short) 4, inputAttrTypes, null);
			} catch (Exception e) {

				e.printStackTrace();
			}

			int eqOff = -1;
			if ((operator[0] == 2 || operator[0] == 3) && (operator[1] == 2 || operator[1] == 3)){
				eqOff = 0;
			}

			else
				eqOff = 1;



			boolean ascR = false, ascS = false;
			if (ascFlagL1 && ascFlagL2)
				ascR = true;
			else
				ascR = false;

			if (ascFlagL1_ && ascFlagL2_)
				ascS = true;
			else
				ascS = false;

			/*if (n > 300)
				return;*/


			int[] B = new int[n];
			int off2= -1, off1 = -1;
			int i=0, k=0, k_=0, j=0;
			System.out.println("Output: ");
			System.out.println("--------------------------");
			System.out.println("T1(ID)  T2(ID)");
			System.out.println("--------------------------");
			for (i = 0; i<= m-1 && tupR !=null ; i++){

				// Condition 1
				//System.out.println(i);
				off2 = o2[i];
				if (operator[0] == 2 && operator[1] == 2 ){
					for (j = off2; j<=n-1  ; j++){
						B[P_[j]] = 1;
					}
				}else if (operator[0] == 3 && operator[1] == 3 ){
					for (j = off2; j<=n-1  ; j++){
						B[P_[j]] = 1;
					}
				}else if (operator[0] == 3 && operator[1] == 2 ){
					for (j = off2; j<=n-1  ; j++){
						B[P_[j]] = 1;
					}
				}else{

					if ((operator[0] == 4 || operator [0] == 1 || operator [0] == 3 || operator [0] == 2) && (operator[1] == 4 || operator [1] == 1 || operator [1] == 2 )){
						for (j = 0; j<=off2-1  ; j++){
							B[P_[j]] = 1;
						}
					}else if ((operator[0] == 2 || operator[0] == 1) && (operator[1] == 3)){
						for (j = off2; j<=n-1  ; j++){
							B[P_[j]] = 1;
						}
					}
				}

				// Condition 1
				off1 = o1[P[i]];
				if (operator[0] == 3 && operator[1] == 3){
					for(k = 0 ; k <= off1 - eqOff; k++){
						if( B[k] == 1){							
							
							RID key = (RID) ridL1_.get(k);
							Tuple temp = L1_.getRecord(key);
							temp.setHdr((short)4, inputAttrTypes, null);
							tupS = temp;						
							System.out.println(" " + tupR.getIntFld(outputColumns[0]) + "     " + tupS.getIntFld(outputColumns[1])); 
						}
					}
					Arrays.fill(B,0);
				}else if (operator[0] == 3 && operator[1] == 2){
					for(k = 0 ; k <= off1 - eqOff; k++){
						if( B[k] == 1){							
							
							RID key = (RID) ridL1_.get(k);
							Tuple temp = L1_.getRecord(key);
							temp.setHdr((short)4, inputAttrTypes, null);
							tupS = temp;						
							System.out.println(" " + tupR.getIntFld(outputColumns[0]) + "     " + tupS.getIntFld(outputColumns[1])); 
						}
					}
					Arrays.fill(B,0);
				}else{
					
					if ( ((operator[0] == 4 || operator [0] == 1 || operator [0] == 3 ) && (operator[1] == 4 || operator [1] == 1 || operator [1] == 2 || operator [1] == 3))){
						for(k = off1 + eqOff; k <= n-1;k++){
							if( B[k] == 1){
								
								
								RID key = (RID) ridL1_.get(k);
								Tuple temp = L1_.getRecord(key);
								temp.setHdr((short)4, inputAttrTypes, null);
								tupS = temp;						
								System.out.println(" " + tupR.getIntFld(outputColumns[0]) + "     " + tupS.getIntFld(outputColumns[1])); 
							}
						}
					}else if ((operator[0] == 2 || operator[0] == 3) && (operator[1] == 3 || operator[1] == 1 || operator[1] == 4 || operator[1] == 2)){
						for(k = 0 ; k <= off1 - eqOff; k++){
							if( B[k] == 1){							
								
								RID key = (RID) ridL1_.get(k);
								Tuple temp = L1_.getRecord(key);
								temp.setHdr((short)4, inputAttrTypes, null);
								tupS = temp;						
								System.out.println(" " + tupR.getIntFld(outputColumns[0]) + "     " + tupS.getIntFld(outputColumns[1])); 
							}
						}
						Arrays.fill(B,0);
					}
				}
				if (!ascFlagL2)               ////added for (1,3)
					Arrays.fill(B,0);
				tupR = scanL2.get_next();
			}
			
			scanL2.close();
			scanL1_.close();
			
			/***
			 * MAIN ALGORITHM ENDS----------------------------------------------------------------
			 */
			long endTime = System.nanoTime();
			long elapsedTime = (endTime - startTime)/1000000;
			System.out.println("Execution Completed ");
			System.out.println("Time elapsed : " + elapsedTime + " ms");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/***
	 * Function where array is in Ascending
	 * @param i Number whose offset is required
	 * @param l1_ Array in which offset is to be found
	 * @param p  start index
	 * @param r  end index
	 * @return offset index
	 */
	private int getIndexAsc(int i, int[] l1_, int p, int r) {
		int mid = (p + r)/2;
		if (i == l1_[mid])
			return mid;
		if (p == r){
			if (r==0)
				return r;
			if (r == l1_.length - 1)
				return r+1;
			else
				return r;
		}

		else if (i < l1_[mid])
			return getIndexAsc(i, l1_, p, mid);
		else
			return getIndexAsc(i, l1_, mid + 1, r);
	}

	/***
	 * Function where array is in Descending
	 * @param i Number whose offset is required
	 * @param l1_ Array in which offset is to be found
	 * @param p  start index
	 * @param r  end index
	 * @return offset index
	 */

	private int getIndexDesc(int i, int[] l1_, int p, int r) {
		int mid = (p + r)/2;
		if (i == l1_[mid])
			return mid;
		if (p == r){
			if (r==0)
				return r;
			if (r == l1_.length - 1)
				return r+1;
			else
				return r;
		}

		else if (i > l1_[mid])
			return getIndexDesc(i, l1_, p, mid);
		else
			return getIndexDesc(i, l1_, mid + 1, r);
	}


}
