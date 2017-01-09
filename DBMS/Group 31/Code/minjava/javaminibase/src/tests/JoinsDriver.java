package tests;

import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

class R {
    public int r_1;
    public int r_2;
    public int r_3;
    public int r_4;

    public R (int _r_1, int _r_2, int _r_3,int _r_4) {
        r_1  = _r_1;
        r_2  = _r_2;
        r_3  = _r_3;
        r_4  = _r_4;
    }
}

//Define the S schema
class S {
    public int    s_1;
    public int    s_2;
    public int 	s_3;
    public int 	s_4;

    public S (int _s_1, int _s_2, int _s_3,int _s_4) {
        s_1  = _s_1;
        s_2  = _s_2;
        s_3 = _s_3;
        s_4 = _s_4;
    }
}

class JoinsDriver implements GlobalConst {

	public boolean OK = true;
	public boolean FAIL = false;
	
    public String[] selClause =null;
    public String[] relName =null;
    public String[] prdctOne =null;
    public String[] prdctTwo =null;
    public int noOfSel= 0;
    public int noOfRel = 0;
    public int noOfWhrOne;
    public int noOfWhrTwo;
    public String opxPrdctOne =null;
    public String opxPrdctTwo =null;
    public String connClause = null;
    public int opx1 = 0 ;
    public int opx2 = 0;
    public int numrInstance = 0;
    public int numrInstance_attrs = 0;
    public int numsInstance = 0;
    public int numsInstance_attrs = 0;
    public int col1_Prdt1= 0 ;
    public int col2_Prdt1= 0 ;
    public int proj1 = 0;
    public int proj2 = 0;
    public int col1_Prdt2= 0 ;
    public int col2_Prdt2= 0;
    public int noOfPredicate = 0;
    private static String queryFileName;
    private static String dataFileName;


    public void queryReading(){
        String fileName = queryFileName;

        // This will reference one line at a time
        String line = null;
        int count = 1;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            System.out.println("*** Execution for following Query  :");


            while((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                if (count == 1)
                {
                    selClause = line.split("\\s+");
                    noOfSel = selClause.length;
                }
                else if (count == 2)
                {
                    relName = line.split("\\s+");
                    noOfRel = relName.length;
                }
                else if (count == 3)
                {
                    prdctOne = line.split("\\s+");
                    noOfWhrOne = prdctOne.length;
                    opxPrdctOne = prdctOne[1];
                }
                else if (count == 4)
                {
                    connClause = line;
                }
                else if (count == 5)
                {
                    prdctTwo = line.split("\\s+");
                    noOfWhrTwo = prdctTwo.length;
                    opxPrdctTwo = prdctTwo[1];
                }
                else
                {
                    System.out.println("\n\n **** Error: Passed Query is invalid. ");
                    Runtime.getRuntime().exit(1);
                }
                count++;
            }

            if (count - 1 == 3)
            {
                noOfPredicate = 1;
            }
            else if ( count -1  == 5)
            {
                noOfPredicate = 2;
            }
            else
            {
                System.out.println("\n\n **** Error: Invalid Number of Predicate. ");
                Runtime.getRuntime().exit(1);
            }

            switch(opxPrdctOne)
            {
                case "1":
                    opx1 = AttrOperator.aopLT;
                    break;
                case "2":
                    opx1 = AttrOperator.aopLE;
                    break;
                case "3":
                    opx1 = AttrOperator.aopGE;
                    break;
                case "4":
                    opx1 = AttrOperator.aopGT;
                    break;
                default:
                    System.out.println("*** Error: Invalid operator in where clause");
                    break;
            }

            if (noOfPredicate == 2)
            {
                switch(opxPrdctTwo)
                {
                    case "1":
                        opx2 = AttrOperator.aopLT;
                        break;
                    case "2":
                        opx2 = AttrOperator.aopLE;
                        break;
                    case "3":
                        opx2 = AttrOperator.aopGE;
                        break;
                    case "4":
                        opx2 = AttrOperator.aopGT;
                        break;
                    default:
                        System.out.println("*** Error: Invalid operator in where clause");
                        break;
                }
                col1_Prdt2 = Integer.parseInt(prdctTwo[0].split("_")[1]);
                col2_Prdt2 = Integer.parseInt(prdctTwo[2].split("_")[1]);
            }


            col1_Prdt1 = Integer.parseInt(prdctOne[0].split("_")[1]);
            col2_Prdt1 = Integer.parseInt(prdctOne[2].split("_")[1]);

            proj1 = Integer.parseInt(selClause[0].split("_")[1]);
            proj2 = Integer.parseInt(selClause[1].split("_")[1]);

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
    }

  
    /** Constructor
     */
    public JoinsDriver() {

       queryReading();
    }

    public boolean runTests() {

        //Disclaimer();

        Query2();

        System.out.print ("Finished joins testing"+"\n");

        return true;
    }

    private void Query1_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(opx1);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),col1_Prdt1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),col2_Prdt1);
        expr[1] = null;
    }

    private void Query2_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(opx1);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),col1_Prdt1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),col2_Prdt1);

        expr[1].op    = new AttrOperator(opx2);
        expr[1].next  = null;
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),col1_Prdt2);
        expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),col2_Prdt2);

        expr[2] = null;
    }

    public void Query2() {

        boolean status = OK;
        System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join )\n");

        CondExpr[] outFilter = new CondExpr[3];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[2] = new CondExpr();

        if(noOfPredicate == 1)
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
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2);

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

        System.out.println("\n\n***** Tuple count for Query 2: ["+q2Count+"]");

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

    private void Disclaimer() {
        System.out.print ("\n\nAny resemblance of persons in this database to"
                + " people living or dead\nis purely coincidental. The contents of "
                + "this database do not reflect\nthe views of the University,"
                + " the Computer  Sciences Department or the\n"
                + "developers...\n\n");
    }

    /**
     * Runs a join query based on the input
     * @param argv {data_file_name, query_file_name, (1a|1b|2a|2b|2c|2d)}
     */
    public static void main(String argv[]){
        if(argv.length<2){
            System.out.println("Please provide data file name, query file name and query type(1a,1b,2a,2b,2c,2d)");
            return;
        }
        queryFileName = argv[1];
        dataFileName = argv[0];
        JoinsDriver jd = new JoinsDriver();

        String queryType = argv[2];
        if(queryType.equals("1a"))
        {
        	long startTime = System.nanoTime();
        	IEJoinTask1 a = new IEJoinTask1(jd);
        	a.runTests();
        	long endTime = System.nanoTime();
            long elapsedTime = (endTime - startTime)/1000000;
    		System.out.println("Execution Completed ");
    		System.out.println("Time elapsed : " + elapsedTime + " ms");
        }
        else if(queryType.equals("1b"))
        {
        	long startTime = System.nanoTime();
        	IEJoinTask1 a = new IEJoinTask1(jd);
        	a.runTests();
        	long endTime = System.nanoTime();
            long elapsedTime = (endTime - startTime)/1000000;
    		System.out.println("Execution Completed ");
    		System.out.println("Time elapsed : " + elapsedTime + " ms");
        }
        else if(queryType.equals("2a"))
        {
           // IEJoinDriver ie= new IEJoinDriver(argv[0], new int[]{jd.proj1}, new int[]{jd.col1_Prdt1}, new int[]{jd.opx1});
        	IEJoinDriver ie= new IEJoinDriver(argv[0], new int[]{jd.proj1}, new int[]{jd.col1_Prdt1}, new int[]{Integer.parseInt(jd.opxPrdctOne)});
        }
        else if(queryType.equals("2b"))
        {
           // IEJoinDriver ie= new IEJoinDriver(argv[0], new int[]{jd.proj1}, new int[]{jd.col1_Prdt1, jd.col1_Prdt2},
                   // new int[]{Integer.parseInt(jd.opxPrdctOne), Integer.parseInt(jd.opxPrdctTwo)});
        	if(jd.connClause.equals("OR")){
                new IEJoinDriver(argv[0], new int[]{jd.proj1}, new int[]{jd.col1_Prdt1}, new int[]{Integer.parseInt(jd.opxPrdctOne)});
                new IEJoinDriver(argv[0], new int[]{jd.proj1}, new int[]{jd.col1_Prdt2}, new int[]{Integer.parseInt(jd.opxPrdctTwo)});
            }else{
                IEJoinDriver ie= new IEJoinDriver(argv[0], new int[]{jd.proj1}, new int[]{jd.col1_Prdt1, jd.col1_Prdt2},
                        new int[]{Integer.parseInt(jd.opxPrdctOne), Integer.parseInt(jd.opxPrdctTwo)});
            }
        }
        else if(queryType.equals("2c"))
        {
        	IEJoinDriver_2c ie = new IEJoinDriver_2c(jd);   
        }
        else if(queryType.equals("2d"))
        {
        	IEJoinDriver_2d ie = new IEJoinDriver_2d(jd);
        }


    }
}