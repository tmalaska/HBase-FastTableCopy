package org.cloudera.sa.hbaseFastSmallToLargeTableCopy;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
      if (args.length == 0) {
        System.out.println("commends:");
        System.out.println("CreateTables: This will create tables");
        System.out.println("PopulateSmallTable: This will populate the small tables");
        System.out.println("CopyDataFromSmallToLargeTable: generate HFiles for Large Table");
        System.out.println("ImportToLargerTableMain: import generated HFiles into Large Table");
      }
      String command = args[0];
      
      String[] subArgs = new String[args.length - 1];
      System.arraycopy(args, 1, subArgs, 0, args.length - 1);
      
      if (command.equals("CreateTables")) {
        CreateTablesMain.main(subArgs);
      } else if (command.equals("PopulateSmallTable")) {
        PopulateSmallTableMain.main(subArgs);
      } else if (command.equals("CopyDataFromSmallToLargeTable")) {
        CopyDataFromSmallToLargeTableMain.main(subArgs);
      } else if (command.equals("ImportToLargerTableMain")) {
        ImportToLargerTableMain.main(subArgs);
      } else {
        System.out.println("Involve command:" + command);
      }
    }
}
