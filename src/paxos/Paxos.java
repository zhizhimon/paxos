package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashMap;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing
    int g_seq;
    Object value;
    int[] done;
    HashMap<Integer, AcceptorStat> map;
    int maxSeq;

    // Your data here
    public class AcceptorStat{

        public int n_p;
        public int n_a;
        public Object v_a;
        public State state;

        public AcceptorStat(){
            this.n_p = -1;
            this.n_a = -1;
            this.v_a = null;
            this.state = State.Pending;
        }
    }


    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);
        this.map = new HashMap<>();
        this.maxSeq = -1;
        this.done = new int[peers.length];
        for(int i=0;i<peers.length;i++){
            this.done[i] = -1;
        }
        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            System.out.println("register for:"+this.me);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            System.out.print("me:"+this.me+" ");
            System.out.println("success");
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
        if(seq<Min())
            return;
        this.g_seq = seq;
        this.value = value;
        Thread obj = new Thread(this);
        obj.start();

    }

    @Override
    public void run(){
        //Your code here
        int seq;
        Object value;
        AcceptorStat acp = new AcceptorStat();
        mutex.lock();
        try{
            seq = this.g_seq;
            value = this.value;
            if(seq>this.maxSeq)
                this.maxSeq = seq;
            map.put(seq, acp);
        }
        finally{
            mutex.unlock();
        }
        int n = -1;
        while(acp.state!=State.Decided) {


            mutex.lock();
            try {
                n = acp.n_p + this.me + 1;
                map.put(seq, acp);
            }
            finally {
                mutex.unlock();
            }
            //
            Request req = new Request(seq, value, n);
            int max_n_a = -1;
            Object max_v_a = null;
            int count_prepare = 0;
            for(int i = 0;i<this.peers.length;i++){
                Response resp = null;
                if(i==this.me){
                    resp = this.Prepare(req);
                }
                else
                    resp = Call("Prepare", req, i);

                if(resp!=null && resp.isPrepareOK == true){
                    if(resp.n_a>max_n_a){
                        max_n_a = resp.n_a;
                        max_v_a = resp.v_a;
                    }
                    count_prepare++;
                }
            }
            if(count_prepare>this.peers.length/2){
                Object v_prepared = null;
                if(max_v_a!=null) {
                    v_prepared = max_v_a;
                }
                else{
                    v_prepared = value;
                }
                int count_accept = 0;
                //Request accept_req = new Request(seq, v_prepared, n);
                req.value = v_prepared;
                for(int i = 0;i<this.peers.length;i++){
                    Response resp = null;
                    if(i==this.me){
                        resp = this.Accept(req);
                    }
                    else {
                        resp = Call("Accept", req, i);
                    }
                    if(resp!=null && resp.isAccept){
                        count_accept++;
                    }
                }
                if(count_accept>this.peers.length/2){
                     for(int i=0;i<this.peers.length;i++){
                         Response resp = null;
                         if(i==this.me){
                             resp = this.Decide(req);
                         }
                         else
                             resp = Call("Decide", req, i);
                          mutex.lock();
                          try {
                              if(resp!=null) {
                                  this.done[i] = resp.latestDone;
                              }
                          }
                          finally {
                             mutex.unlock();
                          }

                     }
                     break;
                }
            }
            mutex.lock();
            try {
                acp = map.get(seq);
            }
            finally {
                mutex.unlock();
            }
        }
    }

//    // Your constructor and methods here
    public Response Prepare(Request req){
        // your code here
        int n = req.number;
        int seq = req.seq;
        boolean isPrepare = false;
        AcceptorStat acp = null;
        mutex.lock();
        try {
            if(seq>maxSeq)
                maxSeq = seq;
            if(this.map.containsKey(seq)) {
                acp = map.get(seq);
                if (n > acp.n_p) {
                    acp.n_p = n;
                    map.put(seq, acp);
                    isPrepare = true;
                }
            }
            else{
                acp = new AcceptorStat();
                acp.n_p = n;
                map.put(seq, acp);
                isPrepare = true;
            }
        }
        finally {
            mutex.unlock();
        }
        Response resp = new Response(isPrepare, false, acp.n_a, acp.v_a, -1);

        return resp;

    }


    public Response Accept(Request req){
        // your code here
        int n = req.number;
        int seq = req.seq;
        Object v = req.value;
        boolean isAccept = false;
        AcceptorStat acp = null;
        mutex.lock();
        try {
            if(seq>maxSeq)
                maxSeq = seq;
            if(this.map.containsKey(seq)) {
                acp = map.get(seq);
                if (n >=acp.n_p) {
                    acp.n_p = n;
                    acp.n_a = n;
                    acp.v_a = v;
                    map.put(seq, acp);
                    isAccept = true;
                }
            }
            else{
                acp = new AcceptorStat(); //????
            }
        }
        finally {
            mutex.unlock();
        }
        Response resp = new Response(false, isAccept, acp.n_a, acp.v_a, -1);
        return resp;
    }

    public Response Decide(Request req){
        // your code here
        AcceptorStat acp = null;
        int seq = req.seq;
        int latestDone = -1;
        mutex.lock();
        try {
            if(seq>maxSeq)
                maxSeq = seq;
            if(this.map.containsKey(seq)) {
                acp = map.get(seq);
            }
            else{
                acp = new AcceptorStat(); //????
            }

            acp.state = State.Decided;
            acp.n_a = req.number;
            acp.v_a = req.value;
            latestDone = this.done[this.me];
            map.put(req.seq, acp);
        }
        finally {
            mutex.unlock();
        }
        return new Response(true,true,acp.n_a,acp.v_a,latestDone);

    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        mutex.lock();
        try {
           int old_done = this.done[this.me];
           if(seq>old_done) {
               this.done[this.me] = seq;
           }
        }
        finally {
            mutex.unlock();
        }

    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        int ret = -1;
        mutex.lock();
        // do not need
        try {
           ret = maxSeq;
        }
        finally {
            mutex.unlock();
        }
        return ret;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().
     *
     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.
     *
     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.
     *
     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        int min = Integer.MAX_VALUE;
        mutex.lock();
        // do not need
        try {
            for(int i=0;i<peers.length;i++){
                if(this.done[i]<min)
                    min = this.done[i];
            }
        }
        finally {
            mutex.unlock();
        }
        return min+1;

    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        AcceptorStat acp = null;
        int min = Min();
        if(seq<min) {
            return new retStatus(State.Forgotten, null);
        }

        mutex.lock();
        try {
            if(this.map.containsKey(seq)) {
                acp = map.get(seq);
            }
            else{
                acp = new AcceptorStat(); //????
            }
        }
        finally {
            mutex.unlock();
        }
        retStatus ret = new retStatus(acp.state,acp.v_a);
        return ret;

    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
