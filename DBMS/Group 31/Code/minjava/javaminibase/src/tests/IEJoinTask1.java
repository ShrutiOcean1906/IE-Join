package tests;
//originally from : joins.C

import iterator.*;
import tests.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

/**
 Here is the implementation for the tests. There are N tests performed.
 We start off by showing that each operator works on its own.
 Then more complicated trees are constructed.
 As a nice feature, we allow the user to specify a selection condition.
 We also allow the user to hardwire trees together.
 */



/* Defined R and S relation considering following assumptions
 * 1. Only 4 columns will exist
 * 2. All Columns will be Integer
 * 3. Only two relation will exist
 */



class IEJoinTask1 implements GlobalConst {

	public Vector rInstance;
	public Vector sInstance;
	public boolean OK = true;
	public boolean FAIL = false;
	JoinsDriver objJd ; 
	
   public void loadData()
    {
	  /* Data Loading in Heap Files */
        if (objJd.noOfRel > 2)
        {
            System.out.println("\n\n **** Error: Invalid Number of Relations ");
            Runtime.getRuntime().exit(1);
        }

        // Loading First Relation in Heap
        String relName1 = objJd.relName[0]+".txt";
        String relName2 = objJd.relName[1]+".txt";
        String dataRel1 = null;
        String[] dataAttr = null;
        int noOfRec1 = 0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader1 =
                    new FileReader(relName1);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader1 =
                    new BufferedReader(fileReader1);

            while((dataRel1 = bufferedReader1.readLine()) != null) {
                dataAttr = dataRel1.split(",");
                if(noOfRec1 > 0)
                    rInstance.addElement(new R( Integer.parseInt(dataAttr[0]),Integer.parseInt(dataAttr[1])
                            ,Integer.parseInt(dataAttr[2]),Integer.parseInt(dataAttr[3])));
                noOfRec1++;
            }
            noOfRec1--;
            // Always close files.
            bufferedReader1.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            relName1 + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + relName1 + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }

        // Loading First Relation in Heap
        String dataRel2 = null;
        String[] dataAttr2 = null;
        int noOfRec2 = 0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader2 =
                    new FileReader(relName2);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader2 =
                    new BufferedReader(fileReader2);

            while((dataRel2 = bufferedReader2.readLine()) != null) {
                dataAttr2 = dataRel2.split(",");
                if(noOfRec2 > 0)
                    sInstance.addElement(new S( Integer.parseInt(dataAttr2[0]),Integer.parseInt(dataAttr2[1])
                            ,Integer.parseInt(dataAttr2[2]),Integer.parseInt(dataAttr2[3])));
                noOfRec2++;
            }
            noOfRec2--;
            // Always close files.
            bufferedReader2.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            relName2 + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + relName2 + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }

//	    rInstance.addElement(new R(55555,645,1,4));
//	    sInstance.addElement(new S(66666,623,2,3));

        objJd.numrInstance = noOfRec1;
        objJd.numrInstance_attrs = 4;
        objJd.numsInstance = noOfRec2;
        objJd.numsInstance_attrs = 4;

    }
    /** Constructor
     */
    public IEJoinTask1(JoinsDriver objJd) {

        rInstance  = new Vector();
        sInstance = new Vector();
        this.objJd =objJd;

	/* This Function will Load Relation Data */
        loadData();

        boolean status = OK;

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

   /* try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    }
    catch (IOException e) {
      System.err.println (""+e);
    }
*/

    /*
    ExtendedSystemDefs extSysDef =
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

        SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

        // creating the rInstance relation
        AttrType [] R_types = new AttrType[4];
        R_types[0] = new AttrType (AttrType.attrInteger);
        R_types[1] = new AttrType (AttrType.attrInteger);
        R_types[2] = new AttrType (AttrType.attrInteger);
        R_types[3] = new AttrType (AttrType.attrInteger);

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 4,R_types, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "rInstance"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("rInstance.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 4, R_types, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<objJd.numrInstance; i++) {
            try {
                t.setIntFld(1, ((R)rInstance.elementAt(i)).r_1);
                t.setIntFld(2, ((R)rInstance.elementAt(i)).r_2);
                t.setIntFld(3, ((R)rInstance.elementAt(i)).r_3);
                t.setIntFld(4, ((R)rInstance.elementAt(i)).r_4);
            }
            catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for rInstance");
            Runtime.getRuntime().exit(1);
        }


        //creating the S relation
        AttrType [] S_Types = new AttrType[4];
        S_Types[0] = new AttrType (AttrType.attrInteger);
        S_Types[1] = new AttrType (AttrType.attrInteger);
        S_Types[2] = new AttrType (AttrType.attrInteger);
        S_Types[3] = new AttrType (AttrType.attrInteger);

        t = new Tuple();
        try {
            t.setHdr((short) 4,S_Types, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        size = t.size();

        // inserting the tuple into file "boats"
        //RID             rid;
        f = null;
        try {
            f = new Heapfile("sInstance.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 4, S_Types, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<objJd.numsInstance; i++) {
            try {
                t.setIntFld(1, ((S)sInstance.elementAt(i)).s_1);
                t.setIntFld(2, ((S)sInstance.elementAt(i)).s_2);
                t.setIntFld(3, ((S)sInstance.elementAt(i)).s_3);
                t.setIntFld(4, ((S)sInstance.elementAt(i)).s_4);

            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sInstance");
            Runtime.getRuntime().exit(1);
        }

    }

    public boolean runTests() {

        if(null != objJd.connClause )
        {
    	if(objJd.connClause.equals("AND"))
        	Query2();
        else if(objJd.connClause.equals("OR"))
        	Query3();
        }
        else
        	Query2();
        System.out.print ("Finished joins testing"+"\n");

        return true;
    }

    private void Query1_OR_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(objJd.opx1);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),objJd.col1_Prdt2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),objJd.col2_Prdt2);
        expr[1] = null;
    }
    
    private void Query1_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(objJd.opx1);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),objJd.col1_Prdt1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),objJd.col2_Prdt1);
        expr[1] = null;
    }

    private void Query2_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(objJd.opx1);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),objJd.col1_Prdt1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),objJd.col2_Prdt1);

        expr[1].op    = new AttrOperator(objJd.opx2);
        expr[1].next  = null;
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),objJd.col1_Prdt2);
        expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),objJd.col2_Prdt2);

        expr[2] = null;
    }

    public void Query2() {

        boolean status = OK;
        System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join )\n");

        CondExpr[] outFilter = new CondExpr[3];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[2] = new CondExpr();

        if(objJd.noOfPredicate == 1)
            Query1_CondExpr(outFilter);
        else
            Query2_CondExpr(outFilter);

        Tuple t = new Tuple();

        AttrType [] R_types = new AttrType[4];
        R_types[0] = new AttrType (AttrType.attrInteger);
        R_types[1] = new AttrType (AttrType.attrInteger);
        R_types[2] = new AttrType (AttrType.attrInteger);
        R_types[3] = new AttrType (AttrType.attrInteger);


        FldSpec [] R_projection = new FldSpec[4];
        R_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        R_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        R_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        R_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

        FileScan am = null;
        try {
            am  = new FileScan("rInstance.in", R_types, null,
                    (short)4, (short)4,
                    R_projection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for rInstance");
            Runtime.getRuntime().exit(1);
        }

        AttrType [] S_Types = new AttrType[4];
        S_Types[0] = new AttrType (AttrType.attrInteger);
        S_Types[1] = new AttrType (AttrType.attrInteger);
        S_Types[2] = new AttrType (AttrType.attrInteger);
        S_Types[3] = new AttrType (AttrType.attrInteger);

        FldSpec [] proj_list = new FldSpec[2];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), objJd.proj1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), objJd.proj2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInteger);
        jtype[1] = new AttrType (AttrType.attrInteger);

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins (R_types, 4, null,
                    S_Types, 4, null,
                    1000,
                    am, "sInstance.in",
                    outFilter, null, proj_list, 2);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = null;
        int q2Count = 0;
        try {
            while ((t = nlj.get_next()) != null) {
                q2Count++;
                t.print(jtype);
            }
        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
            status = FAIL;
        }

        //System.out.println("\n\n***** Tuple count for Query : ["+q2Count+"]");

        if (status != OK) {
            //bail out
            System.err.println ("*** Error in get next tuple ");
            Runtime.getRuntime().exit(1);
        }

        try {
            nlj.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println ("\n");
        if (status != OK) {
            //bail out
            System.err.println ("*** Error in closing ");
            Runtime.getRuntime().exit(1);
        }
    }

    public void Query3() {

    	q1();
    	q2();

    }
    
    public void q1()
    {
        boolean status = OK;
        System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join )\n");

        CondExpr[] outFilter = new CondExpr[3];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[2] = new CondExpr();


        Query1_CondExpr(outFilter);
        
        Tuple t = new Tuple();

        AttrType [] R_types = new AttrType[4];
        R_types[0] = new AttrType (AttrType.attrInteger);
        R_types[1] = new AttrType (AttrType.attrInteger);
        R_types[2] = new AttrType (AttrType.attrInteger);
        R_types[3] = new AttrType (AttrType.attrInteger);


        FldSpec [] R_projection = new FldSpec[4];
        R_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        R_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        R_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        R_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

        FileScan am = null;
        try {
            am  = new FileScan("rInstance.in", R_types, null,
                    (short)4, (short)4,
                    R_projection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for rInstance");
            Runtime.getRuntime().exit(1);
        }

        AttrType [] S_Types = new AttrType[4];
        S_Types[0] = new AttrType (AttrType.attrInteger);
        S_Types[1] = new AttrType (AttrType.attrInteger);
        S_Types[2] = new AttrType (AttrType.attrInteger);
        S_Types[3] = new AttrType (AttrType.attrInteger);

        FldSpec [] proj_list = new FldSpec[2];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), objJd.proj1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), objJd.proj2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInteger);
        jtype[1] = new AttrType (AttrType.attrInteger);

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins (R_types, 4, null,
                    S_Types, 4, null,
                    1000,
                    am, "sInstance.in",
                    outFilter, null, proj_list, 2);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = null;
        int q2Count = 0;
        try {
            while ((t = nlj.get_next()) != null) {
                q2Count++;
                t.print(jtype);
            }
        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
            status = FAIL;
        }

        //System.out.println("\n\n***** Tuple count for Query : ["+q2Count+"]");

        if (status != OK) {
            //bail out
            System.err.println ("*** Error in get next tuple ");
            Runtime.getRuntime().exit(1);
        }

        try {
            nlj.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println ("\n");
        if (status != OK) {
            //bail out
            System.err.println ("*** Error in closing ");
            Runtime.getRuntime().exit(1);
        }
    }

    public void q2()
    {
        boolean status = OK;
        System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join )\n");

        CondExpr[] outFilter = new CondExpr[3];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[2] = new CondExpr();
        
       	Query1_OR_CondExpr(outFilter);

       	Tuple t = new Tuple();

        AttrType [] R_types = new AttrType[4];
        R_types[0] = new AttrType (AttrType.attrInteger);
        R_types[1] = new AttrType (AttrType.attrInteger);
        R_types[2] = new AttrType (AttrType.attrInteger);
        R_types[3] = new AttrType (AttrType.attrInteger);


        FldSpec [] R_projection = new FldSpec[4];
        R_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        R_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        R_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        R_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

        FileScan am = null;
        try {
            am  = new FileScan("rInstance.in", R_types, null,
                    (short)4, (short)4,
                    R_projection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for rInstance");
            Runtime.getRuntime().exit(1);
        }

        AttrType [] S_Types = new AttrType[4];
        S_Types[0] = new AttrType (AttrType.attrInteger);
        S_Types[1] = new AttrType (AttrType.attrInteger);
        S_Types[2] = new AttrType (AttrType.attrInteger);
        S_Types[3] = new AttrType (AttrType.attrInteger);

        FldSpec [] proj_list = new FldSpec[2];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), objJd.proj1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), objJd.proj2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInteger);
        jtype[1] = new AttrType (AttrType.attrInteger);

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins (R_types, 4, null,
                    S_Types, 4, null,
                    1000,
                    am, "sInstance.in",
                    outFilter, null, proj_list, 2);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = null;
        int q2Count = 0;
        try {
            while ((t = nlj.get_next()) != null) {
                q2Count++;
                t.print(jtype);
            }
        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
            status = FAIL;
        }

        //System.out.println("\n\n***** Tuple count for Query : ["+q2Count+"]");

        if (status != OK) {
            //bail out
            System.err.println ("*** Error in get next tuple ");
            Runtime.getRuntime().exit(1);
        }

        try {
            nlj.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println ("\n");
        if (status != OK) {
            //bail out
            System.err.println ("*** Error in closing ");
            Runtime.getRuntime().exit(1);
        }
    }

}



