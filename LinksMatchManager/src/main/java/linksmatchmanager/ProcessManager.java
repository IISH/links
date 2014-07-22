/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package linksmatchmanager;

/**
 *
 * @author oaz
 */
public class ProcessManager {

    int processCount;
    int max;

    /**
     * 
     */
    public ProcessManager(){
        processCount = 0;
        this.max = max;
    }
     
    /**
     * 
     * @param max 
     */
    public ProcessManager(int max){
        processCount = 0;
        this.max = max;
    }
    
    /**
     * 
     * @return 
     */
    public boolean allowProcess(){
        
        if( processCount < max){
            return true;
        }
        
        return false;
    }
    
    /**
     * 
     * @return 
     */
    public int addProcess() {
        processCount++;
        return processCount;
    }

    /**
     * 
     * @return 
     */
    public int removeProcess() {
        processCount--;
        return processCount;
    }

    /**
     * 
     * @return 
     */
    public int getProcessCount() {
        return processCount;
    }
}