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
                    "    faculty text NOT NULL ,\n" +
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
            //Creating a view that shows the total_points of a student (points achived + points of future tests)
            pstmt = connection.prepareStatement("create view student_total_points as \n" +
                    "select student.student_id,student.credit_points + COALESCE(points_taken,0) total_points\n" +
                    "from   (select student_id, sum(credit_points) points_taken\n" +
                    "from attendees inner join test on attendees.test_id = test.test_id \n" +
                    "and attendees.semester = test.semester\n" +
                    "group by student_id) future_points right outer join student on \n" +
                    "student.student_id = future_points.student_id");
            pstmt.execute();
            //Creating a view that shows points achieved and points goal for every student
            pstmt = connection.prepareStatement("CREATE VIEW student_faculty_points AS \n" +
                    "SELECT student_id, student.faculty, points,credit_points\n" +
                    "from student left join credit_points on student.faculty = credit_points.faculty");
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
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE TABLE IF EXISTS Test");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE TABLE IF EXISTS Student");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE TABLE IF EXISTS Supervisor");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE TABLE IF EXISTS Attendees");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE TABLE IF EXISTS Oversees");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
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

    public static void dropTables() {
        InitialState.dropInitialState();
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS student_total_points");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS student_faculty_points");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Test");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Student");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Supervisor");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Attendees");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Oversees");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
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
            pstmt = con.prepareStatement("SELECT * FROM Test WHERE test_id = ? and Semester = ?");
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
        test.setTime(time);
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
            return ERROR;

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
        //Niv wrote this: TODO: tried to execute a delete query in pg when there is nothing to delete and there wasn't error
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

        //Niv wrote this: TODO: i don't think we need to upadate the Attendees table because the DB does it automaticly
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
                return ERROR;

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
            pstmt = con.prepareStatement("INSERT INTO Attendees VALUES (?,?,?)");
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
            pstmt = con.prepareStatement("SELECT * FROM Attendees WHERE studentID = ? and testID = ? and Semester = ?");
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
            pstmt = con.prepareStatement("DELETE FROM Attendees WHERE studentID = ? and testID = ? and Semester = ?");
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
                    "where supervisor_id = ?) S \n" +
                    "on S.supervisor_id = oversees.supervisor_id\n" +
                    //"where oversees.supervisor_id = ? \n" +
                    "\n");
            pstmt.setInt(1,supervisorID);
            ResultSet results = pstmt.executeQuery();
            if (results.next()){

                int rs = results.getInt("sum");
                results.close();
                return rs;
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
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> students = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT DISTINCT student_id\n" +
                    "FROM (SELECT student_id , COUNT(oversees_student.supervisor_id)\n" +
                    "\tFROM (SELECT * \n" +
                    "\t   \tFROM oversees INNER JOIN attendees ON oversees.test_id = attendees.test_id AND \n" +
                    "\t   \toversees.semester = attendees.semester) oversees_student\n" +
                    "\tGROUP BY student_id , supervisor_id\n" +
                    "\tHAVING COUNT(oversees_student.supervisor_id)>=2) students_more_than_once\n" +
                    "\n");
            ResultSet results = pstmt.executeQuery();
            while (results.next()) {
                students.add(results.getInt("student_id"));
            }
            results.close();
            return students;

        } catch (SQLException e) {
            return students;
        } finally {
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


    public static ArrayList<Integer> testsThisSemester(Integer semester) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> tests_id = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT test_id \n" +
                    "  FROM test\n" +
                    " WHERE semester = ?\n" +
                    " LIMIT 5");

            pstmt.setInt(1,semester);

            ResultSet results = pstmt.executeQuery();
            while (results.next()) {
                tests_id.add(results.getInt("test_id"));
            }
            results.close();
            return tests_id;

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        } finally {
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

    public static Boolean studentHalfWayThere(Integer studentID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> students = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT student_id \n" +
                    "from student_faculty_points\n" +
                    "where student_id = ? and (1.0*credit_points/points)*100 >= 50");
            pstmt.setInt(1,studentID);
            ResultSet results = pstmt.executeQuery();
            if (results.next()) {
                results.close();
                return true;
            }
            results.close();
            return false;


        } catch (SQLException e) {
            return false;
        } finally {
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

    public static Integer studentCreditPoints(Integer studentID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> students = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM student_total_points\n" +
                    "WHERE student_id = ?\n" +
                    "\n");
            pstmt.setInt(1,studentID);
            ResultSet results = pstmt.executeQuery();
            if (results.next()) {
               int points =  results.getInt("total_points");
               results.close();
               return points;
            }
            return 0;

        } catch (SQLException e) {
            return 0;
        } finally {
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

    public static Integer getMostPopularTest(String faculty) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> students = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT attendees.test_id , COUNT(test_id) num_of_students\n" +
                    "FROM attendees \n" +
                    "WHERE student_id IN (SELECT student_id FROM student WHERE faculty = ?)\n" +
                    "GROUP BY test_id\n" +
                    "ORDER BY COUNT(test_id) DESC\n" +
                    "LIMIT 1");
            pstmt.setString(1,faculty);
            ResultSet results = pstmt.executeQuery();
            if (results.next()) {
               int id = results.getInt("test_id");
               results.close();
               return id;
            }
            return 0;

        } catch (SQLException e) {
            return 0;
        } finally {
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

    public static ArrayList<Integer> getConflictingTests() {
        /*
        *********could have done also *************

        SELECT DISTINCT test_id
        FROM test T1
        WHERE (day, time, semester) in (select day, time, semester from test T2 where
			        			         T2.test_id != T1.test_id)
         */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> conflicting_tests = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT DISTINCT T1.test_id  " +
                    " FROM test T1, test T2 " +
                    " WHERE T1.test_id != T2.test_id " +
                    " AND         T1.day = T2.day" +
                    " AND      T1.time = T2.time " +
                    " AND     T1.semester = T2.semester" +
                    " ORDER BY T1.test_id" +
                    " ASC");
            ResultSet results = pstmt.executeQuery();
            while (results.next()) {
                conflicting_tests.add(results.getInt("test_id"));//todo: check the order of the array
                results.close();

            }
            return conflicting_tests;

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        } finally {
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

    public static ArrayList<Integer> graduateStudents() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> graduating_students = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT STP.student_id \n" +
                    "from student_faculty_points SFP inner join student_total_points STP \n" +
                    "on SFP.student_id = STP.student_id\n" +
                    "where STP.total_points >= SFP.points \n" +
                    "LIMIT 5");
            ResultSet results = pstmt.executeQuery();
            while (results.next()) {
                graduating_students.add(results.getInt("test_id"));
                results.close();

            }
            return graduating_students;

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        } finally {
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

    public static ArrayList<Integer> getCloseStudents(Integer studentID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> closed = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("select A1.student_id \n" +
                    "from attendees A1 inner join (select  test_id, semester from attendees where student_id = ?) A2\n" +
                    "on A1.test_id = A2.test_id and A1.semester = A2.semester\n" +
                    "where  ? != A1.student_id\n" +
                    "group by A1.student_id\n" +
                    "having (1.0*count(A1.student_id))>=0.5*(select count(student_id) from attendees where student_id=?)\n" +
                    "order by A1.student_id desc\n" +
                    "limit 10");
            pstmt.setInt(1,studentID);
            pstmt.setInt(2,studentID);
            pstmt.setInt(3,studentID);
            ResultSet results = pstmt.executeQuery();
            while (results.next()) {
                closed.add(results.getInt("student_id"));
                results.close();

            }
            return closed;

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        } finally {
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
}

