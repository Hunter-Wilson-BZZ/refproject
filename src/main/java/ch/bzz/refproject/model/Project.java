package ch.bzz.refproject.model;

import java.time.LocalDate;

/**
 * a project
 * <p>
 * M151: Refproject
 *
 * @author Marcel Suter
 */
public class Project {
    private String projectUUID;
    private Category category;
    private String title;
    private String status;
    private LocalDate startDate;
    private LocalDate endDate;


    /**
     * Gets the projectUUID
     *
     * @return value of projectUUID
     */
    public String getProjectUUID() {
        return projectUUID;
    }

    /**
     * Sets the projectUUID
     *
     * @param projectUUID the value to set
     */

    public void setProjectUUID(String projectUUID) {
        this.projectUUID = projectUUID;
    }

    /**
     * Gets the category
     *
     * @return value of category
     */
    public Category getCategory() {
        return category;
    }

    /**
     * Sets the category
     *
     * @param category the value to set
     */

    public void setCategory(Category category) {
        this.category = category;
    }

    /**
     * Gets the title
     *
     * @return value of title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Sets the title
     *
     * @param title the value to set
     */

    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Gets the startDate
     *
     * @return value of startDate
     */
    public LocalDate getStartDate() {
        return startDate;
    }

    /**
     * Sets the startDate
     *
     * @param startDate the value to set
     */

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = LocalDate.parse(startDate);
    }

    /**
     * Gets the endDate
     *
     * @return value of endDate
     */
    public LocalDate getEndDate() {
        return endDate;
    }

    /**
     * Sets the endDate
     *
     * @param endDate the value to set
     */

    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = LocalDate.parse(endDate);
    }

    /**
     * Gets the status
     *
     * @return value of status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status
     *
     * @param status the value to set
     */

    public void setStatus(String status) {
        this.status = status;
    }
}