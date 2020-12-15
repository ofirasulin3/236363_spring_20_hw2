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
        //create your tables here
    }

    public static void clearTables() {
        //clear your tables here
    }

    public static void dropTables() {
        InitialState.dropInitialState();
        //drop your tables here
    }

    public static ReturnValue addTest(Test test) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("INSERT INTO Test VALUES (?,?,?,?,?)");
            pstmt.setInt(1,test.getId());
            pstmt.setInt(2, test.getSemester());
            pstmt.setInt(3,test.getTime());
            pstmt.setInt(4,test.getRoom());
            pstmt.setInt(4,test.getDay());
            pstmt.setInt(4,test.getCreditPoints());
            pstmt.execute();
        }catch(SQLException e){
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue== PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
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
        try {
            pstmt = con.prepareStatement("SELECT * FROM Test WHERE ID = ? and Semester = ?");
            pstmt.setInt(1, testID);
            pstmt.setInt(2, semester);
            pstmt.execute();
        }catch(SQLException e){
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return Test.badTest();
            }
            else if
        }

        return new Test();
    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
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
        return OK;
    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        return new Supervisor();
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        return OK;
    }

    public static ReturnValue studentAttendTest(Integer studentID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue studentWaiveTest(Integer studentID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue supervisorOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue supervisorStopsOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        return OK;
    }

    public static Float averageTestCost() {
        return 0.0f;
    }

    public static Integer getWage(Integer supervisorID) {
        return 0;
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

