package worddistancer;

import com.infiauto.datastr.auto.DictionaryAutomaton;
import com.infiauto.datastr.auto.LevenshteinAutomaton;
import javax.swing.*;
import java.util.Collection;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

// 
/**
 *
 * @author oaz
 */
public class LvTree extends Thread {

    private boolean isError = false;
    private String[] list1;
    private String[] list2;
    private JTextArea status;
    private int maxDistance = 1;
    private int minLength = 1;
    private String output;
    private int algorithm;
    String parseError = "";
    private int rules;

    public LvTree(String[] list1, String[] list2, JTextArea status, String minLength, String maxDistance, String output, int algorithm, int rules) {
        this.list1 = list1;
        this.list2 = list2;
        this.status = status;
        this.output = output;
        this.algorithm = algorithm;
        this.rules = rules;

        try {
            this.minLength = Integer.parseInt(minLength);
            this.maxDistance = Integer.parseInt(maxDistance);
        } catch (Exception e) {
            isError = false;
            parseError = e.getMessage();
        }
    }

    @Override
    public void run() {

        // Clear messages
        status.setText("");

        // Check errors
        if (isError) {
            status.append("Fatal error: Probably Parsing Error, Error message: " + parseError + "\n");
            return;
        }

        // Check status
        if (output.isEmpty()) {
            status.append("No output file found.\n");
            return;
        }

        // To ArrayList
        ArrayList<String> wordList = new ArrayList<String>();
        ArrayList<String> treeList = new ArrayList<String>();
        Collections.addAll(wordList, list1);
        Collections.addAll(treeList, list2);

        // Check Length
        if (minLength > 0) {

            // List 1
            for (int i = wordList.size() - 1; i > -1; i--) {
                if (wordList.get(i).length() < minLength) {
                    String nvt = wordList.remove(i);
                }
            }

            // List 2
            for (int i = treeList.size() - 1; i > -1; i--) {
                if (treeList.get(i).length() < minLength) {
                    String nvt = treeList.remove(i);
                }
            }
        }

        // Create file
        FileWriter fileWriter = null; // because of try-catch scope
        File newTextFile = new File(output);

        // Open file
        try {
            fileWriter = new FileWriter(newTextFile);
        } catch (Exception e) {
            status.append("Error, can not open or create file\n");
            return;
        }

        // Building tree
        // Create dictionary
        DictionaryAutomaton dictionary = null;
        LevenshteinAutomaton la = null;

        // user gui
        // and build tree
        if (algorithm == 0) {
            status.append("Innitial elements: " + wordList.size() + "\nThere are " + treeList.size() + " searchable elements\n");
        } // only if tree is used
        else if (algorithm == 1 || algorithm == 2) {
            dictionary = new DictionaryAutomaton(treeList);

            // Build Tree
            status.append("Innitial elements: " + wordList.size() + "\nBuilding levenshtein tree (of " + treeList.size() + " elements)...");
            la = LevenshteinAutomaton.generateLevenshtein(maxDistance);
            status.append("Done\n");
        }
        
        status.append("Calculating...\n");

        int step = 10000;
        int countStep = step;

        // Loop through list 1
        for (int i = 0; i < wordList.size(); i++) {

            // Status information
            if (i == countStep) {
                status.append(i + " of " + wordList.size() + " DONE\n");
                countStep += step;
            }

            /**
             * Algorithms
             * 0 levenshtein
             * 1 levensthein tree
             * 2 levensthein tree with value
             */
            if (algorithm == 0) {

                // Loop through treeList
                for (String s : treeList) {

                    if (rules > 0) {
                        // check eerste letter
                        char char1 = wordList.get(i).substring(0, 1).toCharArray()[0];
                        char char2 = s.substring(0, 1).toCharArray()[0];

                        // has to be the same
                        if (rules <= 5) {
                            if (char1 != char2) {
                                continue;
                            }
                        } // ijy etc..
                        else if (rules <= 10) {
                            if (char1 != char2) {
                            } else if (((char1 == 'j') || (char1 == 'i') || (char1 == 'y'))
                                    && ((char2 == 'j') || (char2 == 'i') || (char2 == 'y'))) {
                            } else if (((char1 == 'c') || (char1 == 'k'))
                                    && ((char2 == 'c') || (char2 == 'k'))) {
                            } else if (((char1 == 's') || (char1 == 'z'))
                                    && ((char2 == 's') || (char2 == 'z'))) {
                            } else {
                                continue;
                            }
                        }

                        // Rest of the chars
                        if (rules == 2 || rules == 7) {
                            if (!wordList.get(i).substring(2, 3).equals(s.substring(2, 3))) {
                                continue;
                            }
                        } else if (rules == 3 || rules == 8) {
                            if (!wordList.get(i).substring(2, 4).equals(s.substring(2, 4))) {
                                continue;
                            }
                        } else if (rules == 4 || rules == 9) {
                            if (!wordList.get(i).substring(2, 5).equals(s.substring(2, 5))) {
                                continue;
                            }
                        } else if (rules == 5 || rules == 10) {
                            if (!wordList.get(i).substring(2, 6).equals(s.substring(2, 6))) {
                                continue;
                            }
                        }
                    }

                    int len = wordList.get(i).length() - s.length();

                    len = len * len;

                    int res = maxDistance * maxDistance;

                    if (len > res) {
                        continue;
                    }

                    int lv = levenshtein(wordList.get(i), s);

                    if (lv > maxDistance) {
                        continue;
                    }

                    try {
                        fileWriter.write(wordList.get(i) + "," + s + "," + lv + "\n");
                    } catch (Exception e) {
                        status.append("Error, cannot write to file, Error message: " + e.getMessage() + "\n");
                        return;
                    }
                }
            } else if (algorithm == 1) {
                Collection<String> cs = la.recognize(wordList.get(i), dictionary);

                for (String s : cs) {
                    try {
                        fileWriter.write(wordList.get(i) + "," + s + "\n");
                    } catch (Exception e) {
                        status.append("Error, cannot write to file, Error message: " + e.getMessage() + "\n");
                        return;
                    }
                }
            } else if (algorithm == 2) {
                Collection<String> cs = la.recognize(wordList.get(i), dictionary);

                for (String s : cs) {
                    try {
                        fileWriter.write(wordList.get(i) + "," + s + "," + levenshtein(wordList.get(i), s) + "\n");
                    } catch (Exception e) {
                        status.append("Error, cannot write to file, Error message: " + e.getMessage() + "\n");
                        return;
                    }
                }
            } else {
                status.append("Algorithm error, Algorithm index = " + algorithm + "\n");
            }
        }
        try {
            fileWriter.close();
        } catch (Exception e) {
            status.append("Error, can not close file, Error message: " + e.getMessage() + "\n");
            return;
        }
        status.append("Done\n");
        return;
    }

    public static int levenshtein(String s, String t) {

        int n = s.length(); // length of s
        int m = t.length(); // length of t

        int p[] = new int[n + 1]; //'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int _d[]; //placeholder to assist in swapping p and d

        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        char t_j; // jth character of t

        int cost; // cost

        for (i = 0; i <= n; i++) {
            p[i] = i;
        }

        for (j = 1; j <= m; j++) {
            t_j = t.charAt(j - 1);
            d[0] = j;

            for (i = 1; i <= n; i++) {
                cost = s.charAt(i - 1) == t_j ? 0 : 1;
                d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
            }

            _d = p;
            p = d;
            d = _d;
        }

        return p[n];
    }
}