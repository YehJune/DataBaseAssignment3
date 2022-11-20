import java.util.Scanner;
import java.sql.*;
import java.io.*;
import java.lang.reflect.Executable;

public class SqlTest3 {
    private Connection connect = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private ResultSetMetaData rsMetaData = null;

    private final String url = "jdbc:postgresql://localhost:5432/postgres";
    private final String user = "postgres";
    private final String password = "2964";

    public static void main(String[] args) throws Exception {
        try{
        SqlTest3 app = new SqlTest3();
        System.out.println("Creating Database Connection");
        app.connection();

        Scanner scan = new Scanner(System.in);

        app.executeQ("DROP table student, college, apply CASCADE;");

          System.out.println("Creating college, student, apply relations");
          // 3개 Table 생성: Create table문 이용
          app.createTables();
          
          System.out.println("Inserting tuples to college, student, apply relations");
          // 3개 Table에 Tuple 생성: Insert문 이용
          app.insertTuples();
          
          System.out.println("Continue? (Enter 1 for continue)");
          scan.nextLine();

        System.out.println("SQL Programming Test");
        System.out.println("Trigger test 1");

        // page14의 trigger R2 생성
        app.executeQ("create or replace function triggerTest1() returns trigger as $$ begin delete from Apply where sID=Old.sID ;return null; end; $$ language 'plpgsql'; create trigger R2 after delete on Student for each row execute procedure triggerTest1(); ");

        // Delete문 실행
        app.executeQ("delete from Student where GPA < 3.5;");

        // Query 1
        app.executeQ("select * from Student order by sID;");
        app.printQ(1, 4);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();

        // Query 2
        app.executeQ("select * from Apply order by sID, cName, major;");
        app.printQ(2, 4);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();

        System.out.println("Trigger test 2");
        // Page 20의 Trigger R4 생성
        app.executeQ("create or replace function triggerTest2() returns trigger as $$ begin IF exists (select * from college where cName = New.cName ) THEN return null; ELSE return New; END IF; end; $$ language'plpgsql';create trigger R4 before insert on college for each row execute procedure triggerTest2();");

        // insert문 실행
        app.executeQ("insert into College values ('UCLA','CA',20000); insert into College values ('MIT','MI',10000);");
        // Query 3
        app.executeQ("select * from College order by cName;");
        app.printQ(3, 3);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();
        //테이블 및 튜플 리셋
        app.executeQ("DROP table student, college, apply CASCADE;");
        app.createTables();
        app.insertTuples();

        System.out.println("View test 1");
        // View CSEE 생성
        app.executeQ("create view CSEE as select sID, cName, major from Apply where major ='CS' or major='EE'");

        // Query 4 실행하고 결과는 적절한 Print문을 이용해 Display
        app.executeQ("select * from CSEE order by sID, cName, major;");
        app.printQ(4, 3);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();

        System.out.println("view test 2");
        // Trigger CSEEinsert 생성
        app.executeQ("create or replace function viewTest2() returns trigger as $$ begin IF New.major='CS' or New.major='EE' THEN insert into apply values (New.sID, New.cName, New.major, null); return New;  ELSE return null; END IF; end; $$language 'plpgsql'; create trigger CSEEinsert instead of insert on CSEE for each row execute procedure viewTest2();");
        // Insert문 실행
        app.executeQ("insert into CSEE values (333, 'UCLA', 'biology');");
        // Query 5 실행하고 결과는 적절한 Print문을 이용해 Display
        app.executeQ("select * from CSEE order by sID, cName, major;");
        app.printQ(5, 3);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();
        // Query 6 실행하고 결과는 적절한 Print문을 이용해 Display
        app.executeQ("select * from apply order by sID, cName, major;");
        app.printQ(6, 3);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();

        System.out.println("View test 3");
        // Insert문 실행
        app.executeQ("insert into CSEE values (333, 'UCLA', 'CS');");
        // Query 7 실행하고 결과는 적절한 Print문을 이용해 Display
        app.executeQ("select * from CSEE order by sID, cName, major;");
        app.printQ(7, 3);
        System.out.println("Continue? (Enter 1 for continue)");
        scan.nextLine();
        // Query 8 실행하고 결과는 적절한 Print문을 이용해 Display
        app.executeQ("select * from Apply order by sID, cName, major;");
        app.printQ(8, 4);

        app.close();
    }catch(SQLException ex){
            throw ex;
        }
    }

    public void connection() throws SQLException {
        connect = DriverManager.getConnection(url, user, password);
        stmt = connect.createStatement();
    }

    public void close() {
        try {
            if (rs != null) {
                rs.close();
            }

            if (stmt != null) {
                stmt.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void createTables() {
        try {
            String createTable = "create table college(cName varchar(20), state char(2), enrollment int);" +
                    "create table student(sID int, sName varchar(20), GPA numeric(2,1), sizeHS int);" +
                    "create table apply(sID int, cName varchar(20), major varchar(20), decision char);";
            stmt.executeUpdate(createTable);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void insertTuples() {
        try {
            String insertTuples = "insert into college values ('Stanford', 'CA', 15000);" +
                    "insert into college values ('Berkeley', 'CA', 36000);" +
                    "insert into college values ('MIT', 'MA', 10000);" +
                    "insert into college values ('Cornell', 'NY', 21000);" +
                    "insert into student values (123, 'Amy', 3.9, 1000);" +
                    "insert into student values (234, 'Bob', 3.6, 1500);" +
                    "insert into student values (345, 'Craig', 3.5, 500);" +
                    "insert into student values (456, 'Doris', 3.9, 1000);" +
                    "insert into student values (567, 'Edward', 2.9, 2000);" +
                    "insert into student values (678, 'Fay', 3.8, 200);" +
                    "insert into student values (789, 'Gary', 3.4, 800);" +
                    "insert into student values (987, 'Helen', 3.7, 800);" +
                    "insert into student values (876, 'Irene', 3.9, 400);" +
                    "insert into student values (765, 'Jay', 2.9, 1500);" +
                    "insert into student values (654, 'Amy', 3.9, 1000);" +
                    "insert into student values (543, 'Craig', 3.4, 2000);" +
                    "insert into apply values (123, 'Stanford', 'CS', 'Y');" +
                    "insert into apply values (123, 'Stanford', 'EE', 'N');" +
                    "insert into apply values (123, 'Berkeley', 'CS', 'Y');" +
                    "insert into apply values (123, 'Cornell', 'EE', 'Y');" +
                    "insert into apply values (234, 'Berkeley', 'biology', 'N');" +
                    "insert into apply values (345, 'MIT', 'bioengineering', 'Y');" +
                    "insert into apply values (345, 'Cornell', 'bioengineering', 'N');" +
                    "insert into apply values (345, 'Cornell', 'CS', 'Y');" +
                    "insert into apply values (345, 'Cornell', 'EE', 'N');" +
                    "insert into apply values (678, 'Stanford', 'history', 'Y');" +
                    "insert into apply values (987, 'Stanford', 'CS', 'Y');" +
                    "insert into apply values (987, 'Berkeley', 'CS', 'Y');" +
                    "insert into apply values (876, 'Stanford', 'CS', 'N');" +
                    "insert into apply values (876, 'MIT', 'biology', 'Y');" +
                    "insert into apply values (876, 'MIT', 'marine biology', 'N');" +
                    "insert into apply values (765, 'Stanford', 'history', 'Y');" +
                    "insert into apply values (765, 'Cornell', 'history', 'N');" +
                    "insert into apply values (765, 'Cornell', 'psychology', 'Y');" +
                    "insert into apply values (543, 'MIT', 'CS', 'N');";
            stmt.executeUpdate(insertTuples);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void executeQ(String q) {
        try {
            rs = stmt.executeQuery(q);
        } catch (SQLException ex) {
        }
    }

    public void printQ(int queryNum, int colNum) {
        System.out.println("Query" + queryNum);
        try {
            rsMetaData = rs.getMetaData();

            // display Column name
            System.out.printf("                 \t");

            for (int i = 1; i < colNum + 1; i++) {
                System.out.printf(rsMetaData.getColumnName(i) + "               \t");
            }
            System.out.println("");
            System.out.println(
                    "---------------------------------------------------------------------------------------------------------");

            // display result
            int tupleNum = 1;
            while (rs.next()) {
                System.out.printf(tupleNum + "               \t");
                for (int i = 1; i < colNum + 1; i++) {
                    String columnValue = rs.getString(i);
                    System.out.printf(columnValue + "               \t");
                }
                System.out.println("");
                tupleNum++;
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        System.out.println("");
    }
}
