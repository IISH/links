package dataset;



/**
 *
 * @author oaz
 */
public final class MinMaxDateSet {

    /**
     * Private fields
     */
    private int     registrationId;
    private String  date;
    private String  typeDate;
    private int     personRole;
    private int     personAgeYear;
    private int     personAgeMonth;
    private int     personAgeWeek;
    private int     personAgeDay;
    private int     personBirthYear;
    private int     personId;
    private int     sourceId;
    private String  registrationDate;
    private int     registrationMaintype;
    private int     registrationMainRole;
    private String  deathDate;
    private String  death;

    
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }


    public int getPersonAgeDay() {
        return personAgeDay;
    }

    public void setPersonAgeDay(int personAgeDay) {
        this.personAgeDay = personAgeDay;
    }


    public int getPersonAgeMonth() {
        return personAgeMonth;
    }

    public void setPersonAgeMonth(int personAgeMonth) {
        this.personAgeMonth = personAgeMonth;
    }


    public int getPersonAgeWeek() {
        return personAgeWeek;
    }

    public void setPersonAgeWeek(int personAgeWeek) {
        this.personAgeWeek = personAgeWeek;
    }


    public int getPersonAgeYear() {
        return personAgeYear;
    }

    public void setPersonAgeYear(int personAgeYear) {
        this.personAgeYear = personAgeYear;
    }


    public int getPersonBirthYear() {
        return personBirthYear;
    }

    public void setPersonBirthYear(int personBirthYear) {
        this.personBirthYear = personBirthYear;
    }


    public int getPersonId() {
        return personId;
    }

    public void setPersonId(int personId) {
        this.personId = personId;
    }


    public int getPersonRole() {
        return personRole;
    }

    public void setPersonRole(int personRole) {
        this.personRole = personRole;
    }


    public String getRegistrationDate() {
        return registrationDate;
    }

    public void setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
    }


    public int getRegistrationId() {
        return registrationId;
    }

    public void setRegistrationId(int registrationId) {
        this.registrationId = registrationId;
    }


    public int getRegistrationMainRole() {
        return registrationMainRole;
    }

    public void setRegistrationMainRole(int registrationMainRole) {
        this.registrationMainRole = registrationMainRole;
    }


    public int getRegistrationMainType() {
        return registrationMaintype;
    }

    public void setRegistrationMaintype(int registrationMaintype) {
        this.registrationMaintype = registrationMaintype;
    }


    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }


    public String getTypeDate() {
        return typeDate;
    }

    public void setTypeDate(String typeDate) {
        this.typeDate = typeDate;
    }


    public String getDeathDate() {
        return deathDate;
    }

    public void setDeathDate( String deathDate) {
        this.deathDate = deathDate;
    }


    public String getDeath() { return death; }

    public void setDeath( String death ) { this.death = death; }

}
