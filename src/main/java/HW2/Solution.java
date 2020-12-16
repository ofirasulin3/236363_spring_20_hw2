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
            pstmt = con.prepareStatement("INSERT INTO Test VALUES (?,?,?,?,?,?)");
            pstmt.setInt(1, test.getId());
            pstmt.setInt(2, test.getSemester());
            pstmt.setInt(3, test.getTime());
            pstmt.setInt(4, test.getRoom());
            pstmt.setInt(5, test.getDay());
            pstmt.setInt(6, test.getCreditPoints());
            pstmt.execute();
        } catch (SQLException e) {
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return BAD_PARAMS;

            } else if (SQLStateNumValue == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ALREADY_EXISTS;

            } else {
                return ERROR;
            }

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
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
            pstmt = con.prepareStatement("SELECT * FROM Test WHERE ID = ? and Semester = ?");
            pstmt.setInt(1, testID);
            pstmt.setInt(2, semester);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return Test.badTest();
        }

        if (rs == null) {   //||!rs.next()) {
            return Test.badTest();
        }


        try {
            id = rs.getInt(1);
            sem = rs.getInt(2);//semester column
            time = rs.getInt(3);
            room = rs.getInt(4);
            day = rs.getInt(5);
            credit_points = rs.getInt(6);
        } catch (SQLException e) {
            return Test.badTest();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();

            }
        }


        //check if it needs to be before finally
        Test test = new Test();
        test.setId(id);
        test.setSemester(sem);
        test.setTime((time));//TODO: should it be like this? and not (time)?
        test.setRoom(room);
        test.setDay(day);
        test.setCreditPoints(credit_points);
        return test;


    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue addStudent(Student student) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("INSERT INTO Student VALUES (?,?,?,?)");
            pstmt.setInt(1, student.getId());
            pstmt.setString(2, student.getName());
            pstmt.setString(3, student.getFaculty());
            pstmt.setInt(4, student.getCreditPoints());
            pstmt.execute();
        } catch (SQLException e) {
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    SQLStateNumValue == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return BAD_PARAMS;
            } else if (SQLStateNumValue == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ALREADY_EXISTS;
            } else {
                return ERROR;
            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return OK;
    }

    public static Student getStudentProfile(Integer studentID) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int id = 0;
        String name = "";
        String faculty = "";
        int creditPoints = 0;
        try {
            pstmt = con.prepareStatement("SELECT * FROM Student WHERE ID = ?");
            pstmt.setInt(1, studentID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return Student.badStudent();
        }

        if (rs == null) {
            return Student.badStudent();
        }
        try {
            id = rs.getInt(1);
            name = rs.getString(2);
            faculty = rs.getString(3);
            creditPoints = rs.getInt(4);
        } catch (SQLException e) {
            return Student.badStudent();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //check if it needs to be before finally
        Student student = new Student();
        student.setId(id);
        student.setName(name);
        student.setFaculty(faculty);
        student.setCreditPoints(creditPoints);
        return student;
        //return new Student();
    }

    public static ReturnValue deleteStudent(Integer studentID) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        //check if student exists:
        try {
            pstmt = con.prepareStatement("SELECT * FROM Student WHERE ID = ?");
            pstmt.setInt(1, studentID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return ERROR;
        }
        if (rs == null) {
            return NOT_EXISTS;
        }

        //Delete from Attend:
        //PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("DELETE FROM Attend WHERE studentID = ?");
            pstmt.setInt(1, studentID);
            pstmt.execute();
        } catch (SQLException e) {
            return ERROR;
        }

        //Delete from Student:
        //PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("DELETE FROM Student WHERE ID = ?");
            pstmt.setInt(1, studentID);
            pstmt.execute();
        } catch (SQLException e) {
            return ERROR;
        }

        //TODO: what's the difference between excecute and executeQuery?

        //TODO: is it possible to us the same pstmt again and again?

        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return OK;
    }

    public static ReturnValue addSupervisor(Supervisor supervisor) {
        return OK;
    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int id = 0;
        String name = "";
        int salary = 0;
        int creditPoints = 0;
        try {
            pstmt = con.prepareStatement("SELECT * FROM Supervisor WHERE ID = ?");
            pstmt.setInt(1, supervisorID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return Supervisor.badSupervisor();
        }

        if (rs == null) {
            return Supervisor.badSupervisor();
        }
        try {
            id = rs.getInt(1);
            name = rs.getString(2);
            salary = rs.getInt(3);
        } catch (SQLException e) {
            return Supervisor.badSupervisor();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //check if it needs to be before finally
        Supervisor supervisor = new Supervisor();
        supervisor.setId(id);
        supervisor.setName(name);
        supervisor.setSalary(salary);
        return supervisor;
        //return new Supervisor();
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        return OK;
    }

    public static ReturnValue studentAttendTest(Integer studentID, Integer testID, Integer semester) {
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        //check if student exists:
        try {
            pstmt = con.prepareStatement("SELECT * FROM Student WHERE ID = ?");
            pstmt.setInt(1, studentID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return ERROR;
        }
        if (rs == null) {
            return NOT_EXISTS;
        }
        //check if test exists:
        try {
            pstmt = con.prepareStatement("SELECT * FROM Test WHERE ID = ?");
            pstmt.setInt(1, testID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return ERROR;
        }
        if (rs == null) {
            return NOT_EXISTS;
        }


        try {
            pstmt = con.prepareStatement("INSERT INTO Attend VALUES (?,?,?)");
            pstmt.setInt(1, studentID);
            pstmt.setInt(2, testID);
            pstmt.setInt(3, semester);
            pstmt.execute();
        } catch (SQLException e) {
            int SQLStateNumValue = Integer.valueOf(e.getSQLState());
            if (SQLStateNumValue == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ALREADY_EXISTS;
            } else {
                return ERROR;
            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return OK;
    }

    public static ReturnValue studentWaiveTest(Integer studentID, Integer testID, Integer semester) {
        //check if student exists:
        Connection con = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = con.prepareStatement("SELECT * FROM Student WHERE ID = ?");
            pstmt.setInt(1, studentID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return ERROR;
        }
        if (rs == null) {
            return NOT_EXISTS;
        }
        //check if test exists:
        try {
            pstmt = con.prepareStatement("SELECT * FROM Test WHERE ID = ?");
            pstmt.setInt(1, testID);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return ERROR;
        }
        if (rs == null) {
            return NOT_EXISTS;
        }
        //check if student attends test:
        try {
            pstmt = con.prepareStatement("SELECT * FROM Attend WHERE studentID = ? and testID = ? and Semester = ?");
            pstmt.setInt(1, studentID);
            pstmt.setInt(1, testID);
            pstmt.setInt(1, semester);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            return ERROR;
        }
        if (rs == null) {
            return NOT_EXISTS;
        }

        //Delete from Attend:
        //PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("DELETE FROM Attend WHERE studentID = ? and testID = ? and Semester = ?");
            pstmt.setInt(1, studentID);
            pstmt.setInt(1, testID);
            pstmt.setInt(2, semester);
            pstmt.execute();
        } catch (SQLException e) {
            return ERROR;
        }

        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
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

