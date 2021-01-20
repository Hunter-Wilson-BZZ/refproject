package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ProjectDao implements Dao<Project, String>{

    @Override
    public List<Project> getAll() {
        List<Project> projectList = new ArrayList<>();
        String sqlQuery = " SELECT Category.title, Project.title, Project.startDate, Project.endDate FROM RefProject.Project " +
        " INNER JOIN RefProject.Category ON Project.categoryUUID = Category.categoryUUID " +
        " WHERE status = 'A'" +
        " ORDER BY Project.title ASC, Project.startDate DESC;";
        try {
            ResultSet resultSet = MySqlDB.sqlSelect(sqlQuery);
            while (resultSet.next()) {
                Project project = new Project();
                Category category = new Category();
                category.setTitle(resultSet.getString("Category.title"));
                project.setCategory(category);
                project.setTitle(resultSet.getString("Project.title"));
                project.setStartDate(resultSet.getString("Project.startDate"));
                project.setEndDate(resultSet.getString("Project.endDate"));
                projectList.add(project);
            }
        } catch (SQLException sqlEx) {
            MySqlDB.printSQLException(sqlEx);
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }

        return projectList;
    }

    @Override
    public Project getEntity(String projectUUID) {
        Project project = new Project();
        String sqlQuery = "SELECT *" +
                " FROM Project" +
                " INNER JOIN Category ON Project.categoryUUID = Category.categoryUUID" +
                " WHERE projectUUID=?";
        Map<Integer, Object> values = new HashMap<>();
        values.put(1, projectUUID);
        try {
            ResultSet resultSet = MySqlDB.sqlSelect(sqlQuery, values);
            if (resultSet.next()) {
                setValues(resultSet, project);
            }
        } catch (SQLException sqlEx) {
            MySqlDB.printSQLException(sqlEx);
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }
        return project;
    }

    @Override
    public Result delete(String projectUUID) {
        String sqlQuery ="DELETE FROM Project" +
                " WHERE projectUUID=?";
        Map<Integer, Object> values = new HashMap<>();
        values.put(1, projectUUID);
        try {
            return MySqlDB.sqlUpdate(sqlQuery, values);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    @Override
    public Result save (Project project){
        Map<Integer, Object> values = new HashMap<>();
        String sqlQuery;
        if (project.getProjectUUID() == null) {
            project.setProjectUUID(UUID.randomUUID().toString());
            sqlQuery = "INSERT INTO Project";
        } else {
            sqlQuery = "REPLACE Project";
        }
        sqlQuery +=
                " SET projectUUID=?," +
                " title=?," +
                " startDate=?," +
                " endDate=?," +
                " categoryUUID=?," +
                " status=?";

        values.put(1, project.getProjectUUID());
        values.put(2, project.getTitle());
        values.put(3, project.getStartDate().toString());
        values.put(4, project.getEndDate().toString());
        values.put(5, project.getCategory().getCategoryUUID());
        values.put(6, project.getStatus());
        try {
            return MySqlDB.sqlUpdate(sqlQuery, values);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    private void setValues(ResultSet resultSet, Project project) throws SQLException {
        Category category = new Category();
        category.setCategoryUUID(resultSet.getString("categoryUUID"));
        project.setProjectUUID(resultSet.getString("projectUUID"));
        project.setTitle(resultSet.getString("title"));
        project.setStartDate(resultSet.getString("startDate"));
        project.setEndDate(resultSet.getString("endDate"));
        project.setStatus(resultSet.getString("status"));
        project.setCategory(category);
    }

    public ProjectDao() {}

}
