/* File LRUK.java */

package bufmgr;

import diskmgr.*;
import global.*;
import java.util.*;
  /**
   * class LRUK is a subclass of class Replacer using LRUK
   * algorithm for page replacement
   */
public class LRUK extends  Replacer {
	 
	//Hashmap to hold the HISTORY of pages - K referenced.
	HashMap<Integer, List<Integer>> hist = new HashMap<Integer, List<Integer>>();
	
	//Hashmap to hold the LAST of page
	HashMap<Integer,Integer> last = new HashMap<Integer,Integer>();
	
	// variable to hold the value of k - default value assigned as 3
  int k = 3 ;
  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

    private int  frames[];
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;
  
  /*Correlation reference period value considered - 5 */
  private int corRefPeriod = 5;

  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */
  private void update(int frameNo, int t)
  {
     if(frameNo != -1)
    	 {
		  	 int index;
		     int correlation_period_of_referenced_page = 0;
		     for ( index=0; index < nframes; ++index )
		        if ( frames[index] == frameNo )
		            break;
		
		    while ( ++index < nframes )
		        frames[index-1] = frames[index];
		        frames[nframes-1] = frameNo;
		        
		    if(t - LAST(frameNo) > corRefPeriod)
		    	{
		    		//calculated the correlation_period_of_referenced_page
            correlation_period_of_referenced_page = LAST(frameNo) - HIST(frameNo,1);
		    		List<Integer> curArraylist = hist.get(frameNo);
		    		List<Integer> list = new ArrayList<Integer>();
		    		int a = 0;
            //added the new value of time to the hashmap
		    		list.add(t);
		    		last.put(frameNo, t);
		    		for(int elem : curArraylist)
	        		if(a++<k-1)
	      					list.add(elem + correlation_period_of_referenced_page); 	
		    		//added the remaining k-1 values to the new list of times
            hist.put(frameNo,list);
		    	}
		    else
		    	last.put(frameNo,t);
      }
  }
  
  /* 
   * getFrames return the array of frames in the buffer pool 
   * */
 public int[] getFrames()
 {
	 return frames;
 }
 
  /**
   * Calling super class the same method
   * Initializing the frames[] with number of buffer allocated
   * by buffer manager
   * set number of frame used to zero
   *
   * @param	mgr	a BufMgr object
   * @see	BufMgr
   * @see	Replacer
   */
    public void setBufferManager( BufMgr mgr )
     {
        super.setBufferManager(mgr);
        frames = new int [ mgr.getNumBuffers() ];
        nframes = 0;
     }

/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LRUK(BufMgr mgrArg)
    {
      super(mgrArg);
      frames = null;
      
      
    }
    //Constructor to take the value of k reference
    public LRUK(BufMgr mgrArg,int ka)
    {
        super(mgrArg);
        frames = null;
        k = ka;
    }
  
  /**
   * calll super class the same method
   * pin the page in the given frame number 
   * move the page to the end of list  
   *
   * @param	 frameNo	 the frame number to pin
   * @exception  InvalidFrameNumberException
   */
 public void pin(int frameNo) throws InvalidFrameNumberException
 {
    super.pin(frameNo);
    update(frameNo,(int)(System.currentTimeMillis()));  
 }

  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using LRUK policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */

 public int pick_victim()
 {
	 int t = (int)(System.currentTimeMillis());
	 try
	 {
			Thread.sleep(10);
	 }
	 catch (InterruptedException ex) {}
	 int numBuffers = mgr.getNumBuffers();
   int frame;
    if ( nframes < numBuffers ) {
        frame = nframes++;
        frames[frame] = frame;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
        if(!hist.containsKey(frame))
        {
          //created the new space for the given frame in hashmap to hold its K times
        	hist.put(frame, new ArrayList<Integer>());
        	//added recent time as HIST(p,1)  
          hist.get(frame).add(t);
          //added remaining k-1 values as 0
        	for(int a =0;a<k-1;a++)
        		hist.get(frame).add(0);
        }
        else
        {
        	//if it exists in the hashmap, its K values of given frame are updated
          List<Integer> curArraylist = hist.get(frame);
        	List<Integer> list = new ArrayList<Integer>();	
        	list.add(t);
        	int a = 0;
        	for(int elem : curArraylist)
        	{
        		if(a++<k-1)
      					list.add(elem);
        		else
        			  break;
        	}
        }
        // updated the last value of given frame with recent time
        last.put(frame, t);
        return frame;
    }
    int victim = 0;
    int min  = t;
    for ( int i = 0; i < numBuffers; ++i ) {
         frame = frames[i];
        if ( state_bit[frame].state != Pinned ) {
          // chose the victime according to the algorithm
            if((int)(System.currentTimeMillis()) - back(frame,1) > corRefPeriod && HIST(frame, k) < min )
            	{
	            		state_bit[frame].state = Pinned;
	                (mgr.frameTable())[frame].pin();
	                victim = frame;
	            		min = HIST(frame,k);
	            		update(victim, (int)(System.currentTimeMillis()));
	            		return victim;
            	}
        }
    }
    return -1;
 }
 
 //back function to get last(p) value of given page number
 public int back(int pageNum, int pageref)
 {
	 if(pageref == 1)
		 return 0;
	 if(last.containsKey(pageNum))	
	  		return last.get(pageNum);
	  else
	  	return 0;
 }
 
//LAST function to get last(p) value of given page number
 public int LAST(int pageNum)
	 {
		  if(last.containsKey(pageNum))
		  		return last.get(pageNum);
		  else
		  	return 0;
	 }
//HIST function to get HIST(p,k) value of given page number
public int HIST(int pageNum, int k)
{
	  if(hist.containsKey(pageNum))
	  	{
	  		List<Integer> curArraylist = hist.get(pageNum);
	  		int value = k;
	  		for(int elem : curArraylist)
        	{
        		 value--;
        		 if(value == 0)
        			 return elem;
        	}
	  		return 0;
	  	}
	  else
	  	return 0;
}
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "LRUK"; }
 
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "LRUK REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
    }
    System.out.println();
 }
  
}



