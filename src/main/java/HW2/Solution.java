package HW2;

import HW2.business.*;
import HW2.data.DBConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import HW2.data.PostgreSQLErrorCodes;

import java.util.ArrayList;

import static HW2.business.ReturnValue.*;

public class Solution {
    public static void createTables() {

        InitialState.createInitialState();
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE Test\n" +
                    "(\n" +
                    "    test_id integer NOT NULL,\n" +
                    "    semester integer NOT NULL ,\n" +
                    "    time integer NOT NULL ,\n" +
                    "    room integer NOT NULL ,\n" +
                    "    day integer NOT NULL ,\n" +
                    "    credit_points integer NOT NULL ,\n" +
                    "    PRIMARY KEY (test_id, semester),\n" +
                    "    CHECK (test_id > 0),\n" +
                    "    CHECK (semester >= 1 and semester <=3),\n" +
                    "    CHECK (time >= 1 and time <= 3),\n" +
                    "    CHECK (room > 0),\n" +
                    "    CHECK (day >= 1 and day <= 31),\n" +
                    "    CHECK (credit_points > 0)\n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE Student\n" +
                    "(\n" +
                    "    student_id integer NOT NULL,\n" +
                    "    name text NOT NULL ,\n" +
                    "    faculty integer NOT NULL ,\n" +
                    "    credit_points integer default (0) NOT NULL ,\n" +
                    "    PRIMARY KEY (student_id),\n" +
                    "    CHECK (student_id > 0),\n" +
                    "    CHECK (credit_points>=0)\n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE Supervisor\n" +
                    "(\n" +
                    "    supervisor_id integer NOT NULL,\n" +
                    "    name text NOT NULL ,\n" +
                    "    salary integer NOT NULL ,\n" +
                    "    PRIMARY KEY (supervisor_id),\n" +
                    "    CHECK (supervisor_id > 0),\n" +
                    "    CHECK (salary>=0)\n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE Oversees\n" +
                    "(\n" +
                    "    supervisor_id integer NOT NULL,\n" +
                    "    test_id integer NOT NULL ,\n" +
                    "    semster integer NOT NULL ,\n" +
                    "    UNIQUE(supervisor_id, test_id, semester),\n" +
                    "    FOREIGN KEY (supervisor_id) REFERENCES supervisor (supervisor_id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    "    FOREIGN KEY (test_id,semester) REFERENCES test (test_id, semester) ON DELETE CASCADE ON UPDATE CASCADE\n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE Attendees\n" +
                    "(\n" +
                    "    student_id integer NOT NULL,\n" +
                    "    test_id integer NOT NULL ,\n" +
                    "    semster integer NOT NULL ,\n" +
                    "    UNIQUE(student_id, test_id, semester),\n" +
                    "    FOREIGN KEY (student_id) REFERENCES student (student_id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    "    FOREIGN KEY (test_id,semester) REFERENCES test (test_id, semester) ON DELETE CASCADE ON UPDATE CASCADE\n" +
                    ")");
            pstmt.execute();



        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        //create your tables here

    }

    public static void clearTables() {
        //clear your tables here
    }

    public static void dropTables() {
        InitialState.dropInitialState();
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Test");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Student");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Supervisor");
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        //drop your tables here
    }

    public static ReturnValue addTest(Test test) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("INSERT INTO Test VALUES (?,?,?,?,?,?)");
            pstmt.setInt(1,test.getId());
            pstmt.setInt(2, test.getSemester());
            pstmt.setInt(3,test.getTime());
            pstmt.setInt(4,test.getRoom());
            pstmt.setInt(5,test.getDay());
            pstmt.setInt(6,test.getCreditPoints());
            pstmt.execute();
        }catch(SQLException e){
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;

            }
            else if(SQLStateNumValue == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;

            }
            else{
                return ERROR;
            }

        }
        finally {
            try{
                pstmt.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
            try{
                con.close();
            }catch(SQLException e){
                e.printStackTrace();

            }
        }
        return OK;
    }

    public static Test getTestProfile(Integer testID, Integer semester) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int id = 0;
        int sem = 0;
        int time = 0;
        int room = 0;
        int day = 0;
        int credit_points = 0;
        try {
            pstmt = con.prepareStatement("SELECT * FROM Test WHERE test_id = ? and Semester = ?");
            pstmt.setInt(1, testID);
            pstmt.setInt(2, semester);
            rs =  pstmt.executeQuery();
        }catch(SQLException e){
            return Test.badTest();
        }

        if (rs == null){   //||!rs.next()) {
            return Test.badTest();
        }


        try {
            id = rs.getInt(1);
            sem = rs.getInt(2);//semester column
            time = rs.getInt(3);
            room = rs.getInt(4);
            day = rs.getInt(5);
            credit_points = rs.getInt(6);
        }catch(SQLException e){
            return Test.badTest();
        }
        finally{
            try{
                pstmt.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
            try{
                con.close();
            }catch(SQLException e){
                e.printStackTrace();

            }
        }


        //check if it needs to be before finally
        Test test = new Test();
        test.setId(id);
        test.setSemester(sem);
        test.setTime((time));
        test.setRoom(room);
        test.setDay(day);
        test.setCreditPoints(credit_points);
        return test;


    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Test " +

                            "where test_id = ? and semester = ?");
            pstmt.setInt(1,testID);
            pstmt.setInt(1,semester);
            int affectedRows = pstmt.executeUpdate();
            if (affectedRows == 0){
                return NOT_EXISTS;
            }
        } catch (SQLException e) {
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;

            }

            else{
                return ERROR;
            }

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return OK;
    }

    public static ReturnValue addStudent(Student student) {




        return OK;
    }

    public static Student getStudentProfile(Integer studentID) {
        return new Student();
    }

    public static ReturnValue deleteStudent(Integer studentID) {
        return OK;
    }

    public static ReturnValue addSupervisor(Supervisor supervisor) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("INSERT INTO Supervisor VALUES (?,?,?)");
            pstmt.setInt(1,supervisor.getId());
            pstmt.setString(2, supervisor.getName());
            pstmt.setInt(3,supervisor.getSalary());
            pstmt.execute();
        }catch(SQLException e){
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;

            }
            else if(SQLStateNumValue == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;

            }
            else{
                return ERROR;
            }

        }
        finally {
            try{
                pstmt.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
            try{
                con.close();
            }catch(SQLException e){
                e.printStackTrace();

            }
        }
        return OK;



    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        return new Supervisor();
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Supervisor " +

                            "where supervisor_id = ? ");
            pstmt.setInt(1,supervisorID);
            int affectedRows = pstmt.executeUpdate();
            if (affectedRows == 0){
                return NOT_EXISTS;
            }
        } catch (SQLException e) {
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;

            }

            else{
                return ERROR;
            }

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return OK;
    }

    public static ReturnValue studentAttendTest(Integer studentID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue studentWaiveTest(Integer studentID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue supervisorOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("INSERT INTO Oversees VALUES (?,?,?)");
            pstmt.setInt(1,testID);
            pstmt.setInt(2, semester);
            pstmt.setInt(3,supervisorID);
            pstmt.execute();
        }catch(SQLException e){
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;

            }
            else if(SQLStateNumValue == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;

            }
            else{
                return ERROR;
            }

        }
        finally {
            try{
                pstmt.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
            try{
                con.close();
            }catch(SQLException e){
                e.printStackTrace();

            }
        }
        return OK;
    }

    public static ReturnValue supervisorStopsOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Oversees " +

                            "where supervisor_id = ? and test_id = ? and semester = ? ");
            pstmt.setInt(1,supervisorID);
            pstmt.setInt(1,testID);
            pstmt.setInt(1,semester);


            int affectedRows = pstmt.executeUpdate();
            if (affectedRows == 0){
                return NOT_EXISTS;
            }
        } catch (SQLException e) {
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;

            }

            else{
                return ERROR;
            }

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return OK;
    }


    public static Float averageTestCost() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT AVG(tests_cost.average)\n" +
                    "FROM (SELECT test_id , semester , AVG(salary) average\n" +
                    "\t FROM oversees INNER JOIN supervisor on oversees.supervisor_id = supervisor.supervisor_id \n" +
                    "\t GROUP BY test_id , semester) tests_cost\n" +
                    "\t \n" +
                    "\n");
            ResultSet results = pstmt.executeQuery();
            if (results.next()){
                float rs = results.getFloat("avg");
                results.close();
                return  rs;
            }
            else{
                results.close();
                return  Float.valueOf(-1);
            }


        } catch (SQLException e) {
            return Float.valueOf(-1);
        }

        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            }


    }

    public static Integer getWage(Integer supervisorID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("select sum(salary)\n" +
                    "from oversees inner join (select * \n" +
                    "from supervisor \n" +
                    "where supervisor_id = 1) S \n" +
                    "on S.supervisor_id = oversees.supervisor_id\n" +
                    "where oversees.supervisor_id = 1 \n" +
                    "\n");
            ResultSet results = pstmt.executeQuery();
            if (results.next()){
                results.close();
                return results.getInt("sum");
            }


        } catch (SQLException e) {
            return  -1;
        }

        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public static ArrayList<Integer> supervisorOverseeStudent() {


        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> testsThisSemester(Integer semester) {
        return new ArrayList<Integer>();
    }

    public static Boolean studentHalfWayThere(Integer studentID) {
        return true;
    }

    public static Integer studentCreditPoints(Integer studentID) {
        return 0;
    }

    public static Integer getMostPopularTest(String faculty) {
        return 0;
    }

    public static ArrayList<Integer> getConflictingTests() {
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> graduateStudents() {
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> getCloseStudents(Integer studentID) {
        return new ArrayList<Integer>();
    }
}

