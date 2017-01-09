package tests;

import btree.BTreeFile;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by anique on 3/21/16.
 */


class IEJoinDriver extends TestDriver implements GlobalConst {
    private static final int NUM_PAGES = 30;

    //private ArrayList<Q> qTuples = new ArrayList<>();
    private Heapfile inputHeapFile;
    private AttrType[] inputAttrTypes;
    private static final String singlePredOutput = "single_pred_output.txt";

    public IEJoinDriver(String file_name, int[] outputColumns, int[] predicateColumns, int[] predicate){
    	long startTime = System.nanoTime();
        String nameRoot = "ieselfjoin";

        dbpath = "/tmp/"+nameRoot+System.getProperty("user.name")+".minibase-db";
        logpath = "/tmp/"+nameRoot +System.getProperty("user.name")+".minibase-log";
        try {
            //Load the table Q from file to the memory

            FileReader fr = new FileReader(file_name);
            BufferedReader br = new BufferedReader(fr);

            br.readLine(); //Read the file header

            initDB();

            inputHeapFile = new Heapfile("ieselfjoin.in");

            //Intialize a tuple object for inserting into the heapfile
            Tuple t = new Tuple();

            //Define the attributes for the Tuple(Q)
            inputAttrTypes= new AttrType[4];
            for(int i=0;i<inputAttrTypes.length;i++){
                inputAttrTypes[i] = new AttrType(AttrType.attrInteger);
            }

            t.setHdr((short) 4, inputAttrTypes, null);


            int lines_added = 0;

            while(true){
                lines_added++;
                if(lines_added%10000==0){
                    System.out.println(lines_added+" lines added");
                }
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

//                    qTuples.add(new Q( Integer.parseInt(fields[0]),
//                                Integer.parseInt(fields[1]),
//                                Integer.parseInt(fields[2]),
//                                Integer.parseInt(fields[3])
//                            ));
                }else{
                    break;
                }
            }

            //System.out.println("File addition complete");

            br.close();
            fr.close();

            ieSelfJoin(outputColumns,predicateColumns, predicate);


        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long endTime = System.nanoTime();
        long elapsedTime = (endTime - startTime)/1000000;
		System.out.println("Execution Completed ");
		System.out.println("Time elapsed : " + elapsedTime + " ms");
    }

    /**
     * Prints the results of the join operation
     * @param outputColumns An array containing the indices of the columns to be displayed
     * @param predicateColumns An array containing the indices of the columns to be used in predicates
     * @param predicate An array containing the predicate enumeration
     * @return
     */
    private void ieSelfJoin(int[] outputColumns, int[] predicateColumns, int[] predicate){
        try {

            //Heapfile results_heap = new Heapfile("ieselfjoin.out");

            // Specify the columns that we want(Projection)
            FldSpec[] fldSpecs = new FldSpec[outputColumns.length+predicateColumns.length];
            AttrType[] outAttrTypes = new AttrType[fldSpecs.length];

            for(int i=0; i<outputColumns.length; i++){
                fldSpecs[i] = new FldSpec(new RelSpec(RelSpec.outer), outputColumns[i]);
                outAttrTypes[i] = new AttrType(AttrType.attrInteger);
            }

            for(int i=0,j=outputColumns.length; i<predicateColumns.length; i++,j++){
                fldSpecs[j] = new FldSpec(new RelSpec(RelSpec.outer), predicateColumns[i]);
                outAttrTypes[j] = new AttrType(AttrType.attrInteger);
            }


            FileScan fscan = new FileScan("ieselfjoin.in", inputAttrTypes, null, (short)inputAttrTypes.length,
                    fldSpecs.length, fldSpecs, null);

            Heapfile sortedHeap = new Heapfile("ieselfjoinsorted.in");


            // For single predicate
            if(predicate.length == 1){
                Sort sort_asc = new Sort(inputAttrTypes, (short)2, null, fscan, outputColumns.length+1, new TupleOrder(TupleOrder.Ascending),
                        4, (int)Math.floor(NUM_PAGES/2.0) );


                Tuple t = sort_asc.get_next();

                List<RID> sortedRIDList = new ArrayList<RID>();


                while(t!=null){
                    sortedRIDList.add(sortedHeap.insertRecord(t.getTupleByteArray()));

                    t=sort_asc.get_next();
                }

                sort_asc.close();

                Tuple comparisonTuple = new Tuple();
                comparisonTuple.setHdr((short)outAttrTypes.length, outAttrTypes, null);
                Tuple compareeTuple = new Tuple();
                compareeTuple.setHdr((short)outAttrTypes.length, outAttrTypes, null);


                //FileWriter fw = new FileWriter(singlePredOutput);
                //BufferedWriter bw = new BufferedWriter(fw);

                //Now our life is easy
                for(int i = 0; i<sortedRIDList.size(); i++){
                    RID comparisonRID = sortedRIDList.get(i);
                    comparisonTuple.tupleCopy(sortedHeap.getRecord(comparisonRID));

                    switch (predicate[0]){
                        case 1: //<
                            for(int j=i+1;j<sortedRIDList.size(); j++){
                                compareeTuple.tupleCopy(sortedHeap.getRecord(sortedRIDList.get(j)));
                                for(int k=0;k<outputColumns.length;k++){
                                    System.out.print(comparisonTuple.getIntFld(k+1)+","+compareeTuple.getIntFld(k+1));
                                    System.out.print(k==outputColumns.length-1?"":",");
                                }
                                System.out.println();
                            }
                            break;

                        case 2: //<=

                            for(int j=i;j<sortedRIDList.size(); j++){
                                compareeTuple.tupleCopy(sortedHeap.getRecord(sortedRIDList.get(j)));
                                for(int k=0;k<outputColumns.length;k++){
                                    System.out.print(comparisonTuple.getIntFld(k+1)+","+compareeTuple.getIntFld(k+1));
                                    System.out.print(k==outputColumns.length-1?"":",");
                                }
                                System.out.println();
                            }
                            break;
                        case 3: //>=
                            for(int j=i;j>=0; j--){
                                compareeTuple.tupleCopy(sortedHeap.getRecord(sortedRIDList.get(j)));
                                for(int k=0;k<outputColumns.length;k++){
                                    System.out.print(comparisonTuple.getIntFld(k+1)+","+compareeTuple.getIntFld(k+1));
                                    System.out.print(k==outputColumns.length-1?"":",");
                                }
                                System.out.println();
                            }
                            break;

                        default: //>
                            for(int j=i-1;j>=0; j--){
                                compareeTuple.tupleCopy(sortedHeap.getRecord(sortedRIDList.get(j)));
                                for(int k=0;k<outputColumns.length;k++){
                                    System.out.print(comparisonTuple.getIntFld(k+1)+","+compareeTuple.getIntFld(k+1));
                                    System.out.print(k==outputColumns.length-1?"":",");
                                }
                                System.out.println();
                            }
                            break;

                    }


                }

                sortedHeap.deleteFile();
                fscan.close();

                //bw.flush();bw.close();
                //fw.close();


            }else{
                // Sort on the first column

                TupleOrder to_temp = new TupleOrder(TupleOrder.Ascending);
                if(predicate[0]==1||predicate[0]==2)
                    to_temp = new TupleOrder(TupleOrder.Descending);


                Sort sort_asc = new Sort(inputAttrTypes, (short)fldSpecs.length, null, fscan, outputColumns.length+1, to_temp,

                        4, (int)Math.floor(NUM_PAGES/2.0) );



                AttrType indexedAttrType[] = new AttrType[fldSpecs.length+1];

                FldSpec indexedFldSpecs[] = new FldSpec[indexedAttrType.length];


                for (int i=0; i<indexedAttrType.length; i++){

                    indexedAttrType[i] = new AttrType(AttrType.attrInteger);

                    indexedFldSpecs[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1); ///

                }





                Heapfile l1 = new Heapfile("L1");


                Tuple indexedTuple = new Tuple();

                indexedTuple.setHdr((short)(fldSpecs.length+1), indexedAttrType, null);



                // Add index

                Tuple t = sort_asc.get_next();


                List<RID> l1_rid = new ArrayList<>();


                int tuple_index = 0;

                while(t != null){

                    for(int i=1; i<=fldSpecs.length; i++){

                        indexedTuple.setIntFld(i, t.getIntFld(i));

                    }


                    //Add index

                    indexedTuple.setIntFld(fldSpecs.length+1, tuple_index);

                    l1_rid.add(l1.insertRecord(indexedTuple.getTupleByteArray()));


                    t=sort_asc.get_next();

                    tuple_index++;

                }


                sort_asc.close();


                // Sort indexed table on 2nd column

                //Heapfile l2 = new Heapfile("L2");


                FileScan indexedScan = new FileScan("L1", indexedAttrType, null, (short)indexedAttrType.length,

                        indexedFldSpecs.length, indexedFldSpecs, null);


                to_temp = new TupleOrder(TupleOrder.Descending);

                if(predicate[1]==1||predicate[1]==2)
                    to_temp = new TupleOrder(TupleOrder.Ascending);

                Sort sort_indexed = new Sort(inputAttrTypes, (short)indexedAttrType.length, null, indexedScan, outputColumns.length+2,
                        to_temp, 4, (int)Math.floor(NUM_PAGES/2.0) );


                // Create permutation array by linear iteration

                indexedTuple = sort_indexed.get_next();


                List<Integer> permutation_array = new ArrayList<>();

                //List<RID> l2_rid = new ArrayList<>();


                while(indexedTuple!=null){

                    permutation_array.add(indexedTuple.getIntFld(indexedFldSpecs.length)); //Get the index

                    //l2_rid.add(l2.insertRecord(indexedTuple.getTupleByteArray())); //Add to heap file


                    indexedTuple = sort_indexed.get_next();

                }


                sort_indexed.close();

                indexedScan.close();

                int flag_predicate=0;

                //Create bit array

                boolean[] bit_array = new boolean[permutation_array.size()];

                if((predicate[0]==2||predicate[0]==3) && (predicate[1]==2||predicate[1]==3))

                    flag_predicate=0;

                else

                    flag_predicate=1;



                indexedTuple = new Tuple();

                indexedTuple.setHdr((short)(fldSpecs.length+1), indexedAttrType, null);


                for(int i=0; i<permutation_array.size(); i++){

                    //Set this bit as one

                    bit_array[permutation_array.get(i)] = true;


                    //Look at all the proceeding bits in the bit array


                    //TODO EQuality case(Check predicates)

                    for(int j=permutation_array.get(i)+flag_predicate;j<bit_array.length;j++){



                        if(bit_array[j]) {
                            indexedTuple.tupleCopy(l1.getRecord(l1_rid.get(j)));

                            for(int f=1; f<=outputColumns.length; f++){
                                System.out.print(indexedTuple.getIntFld(f)+",");
                            }

                            //print rows at L1[j],L1[perm[i]]
                            RID t_rid = l1_rid.get(permutation_array.get(i));

                            Tuple templ1 = l1.getRecord(t_rid);

                            indexedTuple.tupleCopy(templ1);
                            for(int f=1; f<=outputColumns.length; f++){
                                System.out.print(indexedTuple.getIntFld(f));
                                System.out.print(f==outputColumns.length?"":",");
                            }

                            System.out.println();

                        }

                    }


                }



            }


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void initDB(){
        //System.out.println("Running IESelfJoin Tests...");
        SystemDefs sysdef = new SystemDefs( dbpath, 30000, NUMBUF, "Clock" );

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;
		
		*/

    }

    public boolean runTests(){
        System.out.println("Running IESelfJoin Tests...");
        SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        System.out.println ("\n" + "...IEJOIN tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }

    protected boolean test1(){

        //Define the attributes for the Tuple(Q)
        AttrType[] attrTypes = new AttrType[4];
        for(int i=0;i<attrTypes.length;i++){
            attrTypes[i] = new AttrType(AttrType.attrInteger);
        }

        Tuple t = new Tuple();
        try {
            //Set the Tuple metadata
            t.setHdr((short)4, attrTypes, null);

            //Create a heapfile
            Heapfile f = new Heapfile("ieselfjoin.in");
            Heapfile f_out = new Heapfile("ieselfjoin.out");


            // We want all of Q's four rows
            FldSpec[] fieldProjection = new FldSpec[4];
            fieldProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            fieldProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            fieldProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
            fieldProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);


            //Create a heap file scan for use in sorting
            FileScan fScan = new FileScan("ieselfjoin.in", attrTypes, null, (short)4, 4,fieldProjection ,null);

            t=fScan.get_next();


            while(t!=null){
                System.out.println(t.getIntFld(1)+","+t.getIntFld(2)+","+t.getIntFld(3)+","+t.getIntFld(4));
                t = fScan.get_next();
            }



        } catch (Exception e) {
            e.printStackTrace();
        }


        // TODO Implement this
        return false;
    }

}

public class IESelfJoin {
    public static void main(String[] argv){
        boolean joinStatus = false;

        //IEJoinDriver joinDriver = new IEJoinDriver();

        //joinStatus = joinDriver.runTests();

        if(joinStatus){
            System.out.println("Sucessfully completed all tests");
        }else{
            System.out.println("Some tests were unsuccessful");
        }
    }
}